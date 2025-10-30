import { EventEmitter } from 'events';
import axios from 'axios';
import { IAlertService, IDatabaseManager } from './interfaces';
import { logger } from '../utils/logger';

export interface AlertServiceOptions {
  webhookUrl: string;
  displayNameMap?: Record<string, string>;
  maxRetries?: number;
  retryDelayMs?: number;
}

const ALERT_EMOJI = '🟡';
const JST_FORMATTER = new Intl.DateTimeFormat('ja-JP', {
  timeZone: 'Asia/Tokyo',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
});

export declare interface AlertService {
  on(
    event: 'alertSent',
    listener: (payload: {
      timestamp: number;
      zScore: number;
      deltaZScore: number;
      threshold: number;
      cumulativeValue: number;
      delta: number;
      symbol: string;
      triggerSource: 'cumulative' | 'delta';
      triggerZScore: number;
    }) => void
  ): this;
  on(event: 'alertFailed', listener: (error: Error) => void): this;
}

export class AlertService extends EventEmitter implements IAlertService {
  private readonly databaseManager: IDatabaseManager;
  private readonly webhookUrl: string;
  private readonly displayNameMap: Record<string, string>;
  private readonly maxRetries: number;
  private readonly retryDelayMs: number;

  constructor(databaseManager: IDatabaseManager, options: AlertServiceOptions) {
    super();
    this.databaseManager = databaseManager;
    this.webhookUrl = options.webhookUrl;
    this.displayNameMap = options.displayNameMap ?? {};
    this.maxRetries = options.maxRetries ?? 3;
    this.retryDelayMs = options.retryDelayMs ?? 1_000;
  }

  async sendCvdAlert(payload: {
    timestamp: number;
    zScore: number;
    deltaZScore: number;
    threshold: number;
    cumulativeValue: number;
    delta: number;
    symbol: string;
    triggerSource: 'cumulative' | 'delta';
    triggerZScore: number;
  }): Promise<void> {
    const label = this.displayNameMap[payload.symbol] ?? payload.symbol;
    const direction =
      Math.abs(payload.triggerZScore) >= 1e-8
        ? payload.triggerZScore >= 0
          ? '買い優勢'
          : '売り優勢'
        : payload.cumulativeValue >= 0
        ? '買い優勢'
        : '売り優勢';
    const formattedDelta = Math.abs(payload.delta).toFixed(2);
    const formattedCumulative = payload.cumulativeValue.toFixed(2);
    const formattedZScore = payload.zScore.toFixed(2);
    const formattedDeltaZScore = payload.deltaZScore.toFixed(2);
    const formattedTriggerZ = payload.triggerZScore.toFixed(2);
    const formattedTime = JST_FORMATTER.format(new Date(payload.timestamp));
    const triggerLabel = payload.triggerSource === 'cumulative' ? '累積' : '差分';

    const message = [
      `${ALERT_EMOJI}【Binance CVD Alert】${label}`,
      `時間: ${formattedTime}`,
      `方向: ${direction}`,
      `直近期差分: ${formattedDelta}`,
      `累積出来高差: ${formattedCumulative}`,
      `Zスコア(累積): ${formattedZScore}`,
      `Zスコア(差分): ${formattedDeltaZScore}`,
      `トリガー: ${triggerLabel} (${formattedTriggerZ}) / 閾値: ${payload.threshold}`,
    ].join('\n');

    await this.postWithRetry({ content: message });

    await this.databaseManager.saveAlertHistory({
      alertType: 'CVD_ZSCORE',
      symbol: payload.symbol,
      timestamp: payload.timestamp,
      value: payload.triggerZScore,
      threshold: payload.threshold,
      message,
    });

    this.emit('alertSent', payload);
  }

  private async postWithRetry(body: unknown): Promise<void> {
    let attempt = 0;
    let lastError: unknown;

    while (attempt < this.maxRetries) {
      try {
        await axios.post(this.webhookUrl, body);
        return;
      } catch (error) {
        lastError = error;
        attempt += 1;
        logger.warn(`Failed to post Binance alert (attempt ${attempt})`, error);
        if (attempt < this.maxRetries) {
          await new Promise((resolve) => setTimeout(resolve, this.retryDelayMs));
        }
      }
    }

    logger.error('Exceeded retry attempts for Binance alert webhook', lastError);
    this.emit('alertFailed', lastError as Error);
    throw lastError instanceof Error ? lastError : new Error('Failed to post alert webhook');
  }
}
