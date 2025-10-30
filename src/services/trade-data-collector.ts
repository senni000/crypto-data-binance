import { EventEmitter } from 'events';
import { TradeData, CvdStreamConfig, MarketType } from '../types';
import { IDatabaseManager, ITradeDataCollector } from './interfaces';
import { BinanceTradeWebSocketClient } from './binance-trade-websocket-client';
import { logger } from '../utils/logger';

export interface TradeDataCollectorOptions {
  spotWsUrl: string;
  usdMWsUrl: string;
  coinMWsUrl: string;
  subscriptions: Array<{
    symbol: string;
    marketType: MarketType;
    streamType: NonNullable<CvdStreamConfig['streamType']>;
  }>;
  flushIntervalMs?: number;
  maxBufferSize?: number;
}

export declare interface TradeDataCollector {
  on(event: 'tradeCollectionStarted', listener: () => void): this;
  on(event: 'tradeDataReceived', listener: (trades: TradeData[]) => void): this;
  on(event: 'tradeDataSaved', listener: (count: number) => void): this;
  on(event: 'websocketError', listener: (error: Error) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
}

/**
 * Binance からの約定データを WebSocket で取得しバッファリング・保存する
 */
export class TradeDataCollector extends EventEmitter implements ITradeDataCollector {
  private readonly databaseManager: IDatabaseManager;
  private readonly wsClient: BinanceTradeWebSocketClient;
  private readonly flushIntervalMs: number;
  private readonly maxBufferSize: number;
  private tradeBuffer: TradeData[] = [];
  private flushTimer: NodeJS.Timeout | null = null;
  private running = false;

  constructor(databaseManager: IDatabaseManager, options: TradeDataCollectorOptions) {
    super();
    this.databaseManager = databaseManager;
    this.flushIntervalMs = options.flushIntervalMs ?? 5_000;
    this.maxBufferSize = options.maxBufferSize ?? 1_000;
    this.wsClient = new BinanceTradeWebSocketClient({
      spotUrl: options.spotWsUrl,
      usdMUrl: options.usdMWsUrl,
      coinMUrl: options.coinMWsUrl,
      subscriptions: options.subscriptions,
    });
    this.setupWebSocketHandlers();
  }

  async start(): Promise<void> {
    if (this.running) {
      return;
    }
    this.running = true;

    logger.info('Starting Binance trade data collector');

    try {
      await this.databaseManager.initialize();
      await this.databaseManager.runMigrations();
      await this.wsClient.connect();
      this.startFlushTimer();
      this.emit('tradeCollectionStarted');
    } catch (error) {
      this.running = false;
      logger.error('Failed to start Binance trade collector', error);
      this.emit('error', error as Error);
      throw error;
    }
  }

  async stopCollection(): Promise<void> {
    if (!this.running) {
      return;
    }

    logger.info('Stopping Binance trade data collector');
    this.running = false;
    this.stopFlushTimer();
    this.wsClient.disconnect();
    await this.flushTradeBuffer();
  }

  private setupWebSocketHandlers(): void {
    this.wsClient.on('trade', (trade: TradeData) => {
      this.handleTrade(trade);
    });

    this.wsClient.on('error', (error: Error) => {
      logger.error('Binance trade WebSocket error', error);
      this.emit('websocketError', error);
    });
  }

  private handleTrade(trade: TradeData): void {
    this.tradeBuffer.push(trade);
    this.emit('tradeDataReceived', [trade]);

    if (this.tradeBuffer.length >= this.maxBufferSize) {
      void this.flushTradeBuffer();
    }
  }

  private startFlushTimer(): void {
    this.stopFlushTimer();
    this.flushTimer = setInterval(() => {
      void this.flushTradeBuffer();
    }, this.flushIntervalMs);
  }

  private stopFlushTimer(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
  }

  private async flushTradeBuffer(): Promise<void> {
    if (this.tradeBuffer.length === 0) {
      return;
    }

    const trades = this.tradeBuffer;
    this.tradeBuffer = [];

    try {
      await this.databaseManager.saveTradeData(trades);
      this.emit('tradeDataSaved', trades.length);
    } catch (error) {
      logger.error('Failed to persist Binance trade data', error);
      this.emit('error', error as Error);
      this.tradeBuffer.unshift(...trades);
    }
  }
}
