import { EventEmitter } from 'events';
import { logger } from '../utils/logger';
import {
  IDataCollector,
  IDatabaseManager,
} from './interfaces';
import { BinanceRestClient } from './binance-rest-client';
import { BinanceWebSocketManager } from './binance-websocket-manager';
import { SymbolManager } from './symbol-manager';
import {
  MarketType,
  OHLCVData,
  OHLCVTimeframe,
  TopTraderAccountData,
  TopTraderPositionData,
} from '../types';

interface DataCollectorOptions {
  restIntervals: {
    '30m': number;
    '1d': number;
  };
  topTraderIntervalMs: number;
  bufferFlushIntervalMs: number;
  maxBufferSize: number;
}

const INTERVAL_TO_MS: Record<OHLCVTimeframe, number> = {
  '1m': 60_000,
  '30m': 30 * 60 * 1000,
  '1d': 24 * 60 * 60 * 1000,
};

export declare interface DataCollector {
  on(event: 'restError', listener: (error: Error) => void): this;
  on(event: 'wsError', listener: (error: Error) => void): this;
  on(event: 'ohlcvStored', listener: (payload: { interval: OHLCVTimeframe; count: number }) => void): this;
  on(event: 'topTraderStored', listener: (payload: { positions: number; accounts: number }) => void): this;
}

export class DataCollector extends EventEmitter implements IDataCollector {
  private restTimers: Map<string, NodeJS.Timeout> = new Map();
  private topTraderTimer: NodeJS.Timeout | null = null;
  private bufferTimer: NodeJS.Timeout | null = null;
  private isRunning = false;
  private readonly wsBuffer: OHLCVData[] = [];

  constructor(
    private readonly db: IDatabaseManager,
    private readonly symbolManager: SymbolManager,
    private readonly restClient: BinanceRestClient,
    private readonly wsManager: BinanceWebSocketManager,
    private readonly options: DataCollectorOptions
  ) {
    super();
  }

  async start(): Promise<void> {
    if (this.isRunning) {
      return;
    }
    this.isRunning = true;

    await this.db.initialize();
    await this.db.runMigrations();

    this.wsManager.on('ohlcv', (data) => this.handleWebSocketData(data));
    this.wsManager.on('error', (error) => this.emit('wsError', error));

    this.symbolManager.on('updated', () => {
      logger.info('Symbol list updated. Restarting WebSocket streams.');
      void this.restartWebSockets();
    });
    this.symbolManager.on('error', (error) => this.emit('restError', error));

    await this.symbolManager.updateSymbols();
    this.symbolManager.scheduleDailyUpdate();
    await this.startWebSockets();

    this.scheduleRestCollections();
    this.scheduleTopTraderCollection();
    this.startBufferFlushTimer();

    logger.info('Binance data collector started');
  }

  async stop(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    this.isRunning = false;
    for (const timer of this.restTimers.values()) {
      clearInterval(timer);
    }
    this.restTimers.clear();

    if (this.topTraderTimer) {
      clearInterval(this.topTraderTimer);
      this.topTraderTimer = null;
    }

    if (this.bufferTimer) {
      clearInterval(this.bufferTimer);
      this.bufferTimer = null;
    }

    await this.flushWebSocketBuffer();
    this.wsManager.stop();

    logger.info('Binance data collector stopped');
  }

  private async startWebSockets(): Promise<void> {
    const symbolsByMarket: Record<MarketType, any> = {
      'SPOT': await this.symbolManager.getActiveSymbolsByMarket('SPOT'),
      'USDT-M': await this.symbolManager.getActiveSymbolsByMarket('USDT-M'),
      'COIN-M': await this.symbolManager.getActiveSymbolsByMarket('COIN-M'),
    };
    await this.wsManager.start(symbolsByMarket);
  }

  private async restartWebSockets(): Promise<void> {
    this.wsManager.stop();
    await this.startWebSockets();
  }

  private scheduleRestCollections(): void {
    const collect30m = async () => {
      try {
        await this.collectOHLCV('30m');
      } catch (error) {
        logger.error('Failed to collect 30m OHLCV data', error);
        this.emit('restError', error as Error);
      }
    };
    const collect1d = async () => {
      try {
        await this.collectOHLCV('1d');
      } catch (error) {
        logger.error('Failed to collect 1d OHLCV data', error);
        this.emit('restError', error as Error);
      }
    };

    void collect30m();
    void collect1d();

    this.restTimers.set(
      '30m',
      setInterval(collect30m, this.options.restIntervals['30m'])
    );
    this.restTimers.set(
      '1d',
      setInterval(collect1d, this.options.restIntervals['1d'])
    );
  }

  private scheduleTopTraderCollection(): void {
    const collect = async () => {
      try {
        await this.collectTopTraderData();
      } catch (error) {
        logger.error('Failed to collect top trader data', error);
        this.emit('restError', error as Error);
      }
    };
    void collect();
    this.topTraderTimer = setInterval(collect, this.options.topTraderIntervalMs);
  }

  private startBufferFlushTimer(): void {
    this.bufferTimer = setInterval(() => {
      void this.flushWebSocketBuffer();
    }, this.options.bufferFlushIntervalMs);
  }

  private handleWebSocketData(data: OHLCVData): void {
    this.wsBuffer.push(data);
    if (this.wsBuffer.length >= this.options.maxBufferSize) {
      void this.flushWebSocketBuffer();
    }
  }

  private async flushWebSocketBuffer(): Promise<void> {
    if (this.wsBuffer.length === 0) {
      return;
    }
    const batch = this.wsBuffer.splice(0, this.wsBuffer.length);
    try {
      await this.db.saveOHLCVBatch(batch);
      this.emit('ohlcvStored', { interval: '1m', count: batch.length });
    } catch (error) {
      logger.error('Failed to persist WebSocket OHLCV batch', error);
      this.emit('restError', error as Error);
      this.wsBuffer.unshift(...batch);
    }
  }

  private async collectOHLCV(interval: OHLCVTimeframe): Promise<void> {
    const symbols = await this.symbolManager.getAllActiveSymbols();
    if (symbols.length === 0) {
      return;
    }

    const lastTimestamps = await this.db.getLastOHLCVTimestamps(interval);
    let storedCount = 0;

    for (const symbol of symbols) {
      const last = lastTimestamps[symbol.symbol];
      const startTime = last ? last + INTERVAL_TO_MS[interval] : undefined;
      try {
        const klines = await this.restClient.fetchKlines(symbol.symbol, interval, symbol.marketType, startTime);
        const filtered = startTime
          ? klines.filter((row) => row.openTime >= startTime)
          : klines;
        if (filtered.length > 0) {
          await this.db.saveOHLCVBatch(filtered);
          storedCount += filtered.length;
        }
      } catch (error) {
        logger.error(`Failed to fetch OHLCV for ${symbol.symbol} (${interval})`, error);
        this.emit('restError', error as Error);
      }
    }

    if (storedCount > 0) {
      this.emit('ohlcvStored', { interval, count: storedCount });
    }
  }

  private async collectTopTraderData(): Promise<void> {
    const symbols = await this.symbolManager.getActiveSymbolsByMarket('USDT-M');
    if (symbols.length === 0) {
      return;
    }

    let positionsCount = 0;
    let accountsCount = 0;

    for (const symbol of symbols) {
      try {
        const [positions, accounts] = await Promise.all([
          this.restClient.fetchTopTraderPositions(symbol.symbol),
          this.restClient.fetchTopTraderAccounts(symbol.symbol),
        ]);
        const newPositions = this.filterNewTopTraderEntries(positions);
        const newAccounts = this.filterNewTopTraderEntries(accounts);

        if (newPositions.length > 0) {
          await this.db.saveTopTraderPositions(newPositions as TopTraderPositionData[]);
          positionsCount += newPositions.length;
        }
        if (newAccounts.length > 0) {
          await this.db.saveTopTraderAccounts(newAccounts as TopTraderAccountData[]);
          accountsCount += newAccounts.length;
        }
      } catch (error) {
        logger.error(`Failed to fetch top trader data for ${symbol.symbol}`, error);
        this.emit('restError', error as Error);
      }
    }

    if (positionsCount > 0 || accountsCount > 0) {
      this.emit('topTraderStored', {
        positions: positionsCount,
        accounts: accountsCount,
      });
    }
  }

  private filterNewTopTraderEntries<T extends { timestamp: number }>(entries: T[]): T[] {
    const cutoff = Date.now() - 24 * 60 * 60 * 1000;
    return entries.filter((entry) => entry.timestamp >= cutoff);
  }
}
