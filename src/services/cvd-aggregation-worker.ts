import { EventEmitter } from 'events';
import {
  CumulativeCvdAggregator,
  CumulativeCvdAggregatorOptions,
  CvdAlertPayload,
} from '@crypto-data/cvd-core';
import { IDatabaseManager } from './interfaces';
import { CvdAggregatorConfig, TradeData, TradeDataRow, CvdStreamConfig } from '../types';
import { logger } from '../utils/logger';

const THREE_DAY_WINDOW_MS = 72 * 60 * 60 * 1000;

export interface CvdAggregationWorkerOptions {
  batchSize?: number;
  pollIntervalMs?: number;
  suppressionWindowMinutes?: number;
}

interface ExtendedCvdAlertPayload extends CvdAlertPayload {
  rawThreshold: number;
  logThreshold: number;
  rawTriggerZScore: number;
  logTriggerZScore: number;
  rawZScore: number;
  rawDeltaZScore: number;
  logZScore?: number;
  logDeltaZScore?: number;
}

export declare interface CvdAggregationWorker {
  on(event: 'batchProcessed', listener: (payload: { aggregatorId: string; count: number }) => void): this;
  on(event: 'idle', listener: () => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
}

type NormalizedStream = Required<CvdStreamConfig>;

export class CvdAggregationWorker extends EventEmitter {
  private readonly batchSize: number;
  private readonly pollIntervalMs: number;
  private readonly suppressionWindowMs: number;
  private readonly logZScoreThreshold: number;
  private readonly rawZScoreThreshold: number;
  private readonly historyWindowMs: number;
  private readonly aggregators = new Map<string, CumulativeCvdAggregator>();
  private readonly streamMap = new Map<string, NormalizedStream[]>();
  private running = false;
  private processing = false;
  private timer: NodeJS.Timeout | null = null;

  constructor(
    private readonly databaseManager: IDatabaseManager,
    private readonly aggregatorConfigs: CvdAggregatorConfig[],
    threshold: number,
    private readonly alertsEnabled: boolean,
    options: CvdAggregationWorkerOptions = {}
  ) {
    super();
    this.batchSize = Math.max(1, Math.floor(options.batchSize ?? 500));
    this.pollIntervalMs = Math.max(500, Math.floor(options.pollIntervalMs ?? 2_000));
    this.suppressionWindowMs = Math.max(1, Math.floor((options.suppressionWindowMinutes ?? 30) * 60 * 1000));
    this.logZScoreThreshold = Math.max(threshold, 0.1);
    this.rawZScoreThreshold = Math.exp(this.logZScoreThreshold);
    this.historyWindowMs = THREE_DAY_WINDOW_MS;

    for (const config of aggregatorConfigs) {
      this.streamMap.set(config.id, this.normalizeStreams(config.streams));
    }
  }

  async start(): Promise<void> {
    if (this.running) {
      return;
    }
    this.running = true;
    await this.ensureAggregators();
    await this.processAllAggregators();
    this.scheduleNextIteration();
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    if (this.processing) {
      await this.waitForIdle();
    }

    await Promise.all(
      Array.from(this.aggregators.values()).map(async (aggregator) => {
        try {
          await aggregator.flush();
        } catch (error) {
          logger.error('Failed to flush CVD aggregator during shutdown', error);
        }
      })
    );
  }

  private async ensureAggregators(): Promise<void> {
    for (const config of this.aggregatorConfigs) {
      if (this.aggregators.has(config.id)) {
        continue;
      }

      const aggregator = new CumulativeCvdAggregator(
        {
          repository: {
            loadHistory: (since) => this.databaseManager.getCVDDataSince(config.id, since),
            saveRecord: (record) =>
              this.databaseManager.saveCVDData({
                symbol: config.id,
                timestamp: record.timestamp,
                cvdValue: record.cvdValue,
                zScore: record.zScore,
                delta: record.delta ?? 0,
                deltaZScore: record.deltaZScore ?? 0,
              }),
          },
          alertHandler: {
            sendAlert: (payload) => this.enqueueAlert(config, payload),
          },
          logger,
        },
        this.buildAggregatorOptions(config)
      );

      await aggregator.initialize();
      this.aggregators.set(config.id, aggregator);
    }
  }

  private buildAggregatorOptions(config: CvdAggregatorConfig): CumulativeCvdAggregatorOptions {
    const streams = this.streamMap.get(config.id) ?? [];
    const alertsForAggregator = this.alertsEnabled && config.alertsEnabled !== false;

    const options: CumulativeCvdAggregatorOptions = {
      symbol: config.id,
      threshold: this.rawZScoreThreshold,
      historyWindowMs: this.historyWindowMs,
      tradeFilter: (trade) =>
        streams.some(
          (stream) =>
            stream.symbol === trade.symbol &&
            stream.marketType === (trade as TradeData).marketType &&
            stream.streamType === (trade as TradeData).streamType
        ),
    };

    if (alertsForAggregator) {
      options.suppressionHandler = {
        shouldSuppress: async (payload) => {
          const cutoff = Date.now() - this.suppressionWindowMs;
          return this.databaseManager.hasRecentAlertOrPending('CVD_ZSCORE', payload.symbol, cutoff);
        },
      };
    }

    return options;
  }

  private normalizeStreams(streams: CvdStreamConfig[]): NormalizedStream[] {
    const deduped = new Map<string, NormalizedStream>();
    for (const stream of streams) {
      const normalized: NormalizedStream = {
        symbol: stream.symbol.toUpperCase(),
        marketType: stream.marketType,
        streamType: stream.streamType ?? 'aggTrade',
      };
      const key = `${normalized.marketType}:${normalized.symbol}:${normalized.streamType}`;
      if (!deduped.has(key)) {
        deduped.set(key, normalized);
      }
    }
    return Array.from(deduped.values());
  }

  private scheduleNextIteration(): void {
    if (!this.running) {
      return;
    }
    this.timer = setTimeout(() => {
      void this.processAllAggregators()
        .catch((error) => {
          this.emit('error', error);
          logger.error('CVD aggregation cycle failed', error);
        })
        .finally(() => {
          if (this.running) {
            this.scheduleNextIteration();
          }
        });
    }, this.pollIntervalMs);
  }

  private async processAllAggregators(): Promise<void> {
    if (!this.running || this.processing) {
      return;
    }
    this.processing = true;

    try {
      for (const config of this.aggregatorConfigs) {
        if (!this.running) {
          break;
        }
        await this.processAggregator(config).catch((error) => {
          logger.error(`Failed to process CVD aggregator ${config.id}`, error);
          this.emit('error', error instanceof Error ? error : new Error(String(error)));
        });
      }
      this.emit('idle');
    } finally {
      this.processing = false;
      this.notifyIdle();
    }
  }

  private async processAggregator(config: CvdAggregatorConfig): Promise<void> {
    const aggregator = this.aggregators.get(config.id);
    const streams = this.streamMap.get(config.id) ?? [];
    if (!aggregator || streams.length === 0) {
      return;
    }

    let state = await this.databaseManager.getProcessingState('cvd_aggregator', config.id);
    let lastRowId = state?.lastRowId ?? 0;

    let hasMore = true;
    while (this.running && hasMore) {
      const trades = await this.databaseManager.getTradeDataSinceRowId(streams, lastRowId, this.batchSize);
      if (trades.length === 0) {
        hasMore = false;
        break;
      }

      const payload = trades.map((trade) => this.stripRowMetadata(trade));
      aggregator.enqueueTrades(payload);
      await aggregator.flush();

      lastRowId = trades[trades.length - 1]!.rowId;
      state = {
        lastRowId,
        lastTimestamp: trades[trades.length - 1]!.timestamp,
      };
      await this.databaseManager.saveProcessingState('cvd_aggregator', config.id, state);

      this.emit('batchProcessed', { aggregatorId: config.id, count: trades.length });

      if (trades.length < this.batchSize) {
        hasMore = false;
      }
    }
  }

  private stripRowMetadata(trade: TradeDataRow): TradeData {
    const { rowId: _rowId, ...rest } = trade;
    return rest;
  }

  private async enqueueAlert(config: CvdAggregatorConfig, payload: CvdAlertPayload): Promise<void> {
    if (!this.alertsEnabled || config.alertsEnabled === false) {
      return;
    }
    try {
      const transformed = this.buildAlertPayload(payload);
      if (!transformed) {
        logger.debug('Dropping CVD alert due to insufficient log Z-score', {
          symbol: config.id,
          triggerZScore: payload.triggerZScore,
        });
        return;
      }
      await this.databaseManager.enqueueAlert('CVD_ZSCORE', transformed);
    } catch (error) {
      logger.error('Failed to enqueue CVD alert payload', {
        aggregatorId: config.id,
        error,
      });
      this.emit('error', error instanceof Error ? error : new Error(String(error)));
    }
  }

  private buildAlertPayload(payload: CvdAlertPayload): ExtendedCvdAlertPayload | null {
    const logTriggerZScore = this.toSignedLog(payload.triggerZScore);
    if (Math.abs(logTriggerZScore) < this.logZScoreThreshold) {
      return null;
    }

    const logZScore = this.toSignedLog(payload.zScore);
    const logDeltaZScore = this.toSignedLog(payload.deltaZScore);

    const enriched: ExtendedCvdAlertPayload = {
      ...payload,
      threshold: this.logZScoreThreshold,
      rawThreshold: this.rawZScoreThreshold,
      logThreshold: this.logZScoreThreshold,
      rawTriggerZScore: payload.triggerZScore,
      logTriggerZScore,
      rawZScore: payload.zScore,
      rawDeltaZScore: payload.deltaZScore,
    };

    if (Number.isFinite(logZScore)) {
      enriched.logZScore = logZScore;
    }
    if (Number.isFinite(logDeltaZScore)) {
      enriched.logDeltaZScore = logDeltaZScore;
    }

    return enriched;
  }

  private toSignedLog(value: number): number {
    if (!Number.isFinite(value)) {
      return 0;
    }
    const magnitude = Math.abs(value);
    if (magnitude < 1) {
      return 0;
    }
    const logMagnitude = Math.log(magnitude);
    return value >= 0 ? logMagnitude : -logMagnitude;
  }

  private waiters: Array<() => void> = [];

  private notifyIdle(): void {
    while (this.waiters.length > 0) {
      const resolve = this.waiters.shift();
      resolve?.();
    }
  }

  private waitForIdle(): Promise<void> {
    return new Promise((resolve) => {
      if (!this.processing) {
        resolve();
        return;
      }
      this.waiters.push(resolve);
    });
  }
}
