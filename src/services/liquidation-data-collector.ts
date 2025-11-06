import { EventEmitter } from 'events';
import { IDatabaseManager, ILiquidationDataCollector } from './interfaces';
import { BinanceLiquidationWebSocketClient, BinanceForceOrderEvent } from './binance-liquidation-websocket-client';
import { LiquidationEvent } from '../types';
import { logger } from '../utils/logger';

export interface LiquidationDataCollectorOptions {
  usdMWsUrl: string;
  coinMWsUrl: string;
  subscriptions: Array<{
    symbol: string;
    marketType: Extract<LiquidationEvent['marketType'], 'USDT-M' | 'COIN-M'>;
  }>;
  flushIntervalMs?: number;
  maxBufferSize?: number;
}

export declare interface LiquidationDataCollector {
  on(event: 'liquidationDataReceived', listener: (events: LiquidationEvent[]) => void): this;
  on(event: 'liquidationDataSaved', listener: (count: number) => void): this;
  on(event: 'websocketError', listener: (error: Error) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  on(event: 'started', listener: () => void): this;
}

export class LiquidationDataCollector
  extends EventEmitter
  implements ILiquidationDataCollector {
  private readonly database: IDatabaseManager;
  private readonly wsClient: BinanceLiquidationWebSocketClient | null;
  private readonly flushIntervalMs: number;
  private readonly maxBufferSize: number;
  private readonly subscriptions: LiquidationDataCollectorOptions['subscriptions'];
  private buffer: LiquidationEvent[] = [];
  private flushTimer: NodeJS.Timeout | null = null;
  private running = false;

  constructor(database: IDatabaseManager, options: LiquidationDataCollectorOptions) {
    super();
    this.database = database;
    this.flushIntervalMs = options.flushIntervalMs ?? 5_000;
    this.maxBufferSize = options.maxBufferSize ?? 500;
    this.subscriptions = options.subscriptions;

    if (this.subscriptions.length === 0) {
      this.wsClient = null;
    } else {
      this.wsClient = new BinanceLiquidationWebSocketClient({
        usdMUrl: options.usdMWsUrl,
        coinMUrl: options.coinMWsUrl,
        subscriptions: this.subscriptions,
      });
      this.wsClient.on('liquidation', (payload) => this.handleEvent(payload));
      this.wsClient.on('error', (error) => {
        logger.error('Binance liquidation WS error', error);
        this.emit('websocketError', error);
      });
    }
  }

  async start(): Promise<void> {
    if (this.running) {
      return;
    }

    if (!this.wsClient) {
      logger.info('No Binance liquidation subscriptions configured; skipping collector start');
      return;
    }

    this.running = true;
    logger.info('Starting Binance liquidation data collector', {
      streams: this.subscriptions.length,
    });

    try {
      await this.wsClient.connect();
      this.startFlushTimer();
      this.emit('started');
    } catch (error) {
      this.running = false;
      logger.error('Failed to start Binance liquidation collector', error);
      this.emit('error', error as Error);
      throw error;
    }
  }

  async stop(): Promise<void> {
    if (!this.running) {
      return;
    }

    logger.info('Stopping Binance liquidation data collector');
    this.running = false;
    this.stopFlushTimer();
    this.wsClient?.disconnect();
    await this.flushBuffer();
  }

  private handleEvent(payload: BinanceForceOrderEvent): void {
    const event = this.transform(payload);
    if (!event) {
      return;
    }

    this.buffer.push(event);
    this.emit('liquidationDataReceived', [event]);

    if (this.buffer.length >= this.maxBufferSize) {
      void this.flushBuffer();
    }
  }

  private transform(payload: BinanceForceOrderEvent): LiquidationEvent | null {
    const price = payload.price || payload.averagePrice || payload.lastFilledPrice;
    if (!price) {
      return null;
    }

    const eventId = this.buildEventId(payload);

    const event: LiquidationEvent = {
      eventId,
      symbol: payload.symbol,
      marketType: payload.marketType,
      side: payload.side === 'BUY' ? 'buy' : 'sell',
      price,
      originalQuantity: payload.originalQuantity,
      filledQuantity: payload.filledQuantity,
      eventTime: payload.eventTime,
      createdAt: Date.now(),
    };

    if (payload.orderType !== undefined) {
      event.orderType = payload.orderType;
    }
    if (payload.timeInForce !== undefined) {
      event.timeInForce = payload.timeInForce;
    }
    if (payload.status !== undefined) {
      event.status = payload.status;
    }
    if (payload.orderId !== undefined) {
      event.orderId = payload.orderId;
    }
    if (payload.averagePrice !== undefined) {
      event.averagePrice = payload.averagePrice;
    }
    if (payload.lastFilledPrice !== undefined) {
      event.lastFilledPrice = payload.lastFilledPrice;
    }
    if (payload.lastFilledQuantity !== undefined) {
      event.lastFilledQuantity = payload.lastFilledQuantity;
    }
    if (payload.tradeTime !== undefined) {
      event.tradeTime = payload.tradeTime;
    }
    if (payload.isMaker !== undefined) {
      event.isMaker = payload.isMaker;
    }
    if (payload.reduceOnly !== undefined) {
      event.reduceOnly = payload.reduceOnly;
    }

    return event;
  }

  private buildEventId(payload: BinanceForceOrderEvent): string {
    const base =
      payload.orderId ??
      [
        payload.symbol,
        payload.eventTime,
        payload.tradeTime ?? 0,
        payload.side,
        payload.filledQuantity,
      ].join('-');

    return `${payload.marketType}:${base}`;
  }

  private startFlushTimer(): void {
    this.stopFlushTimer();
    this.flushTimer = setInterval(() => {
      void this.flushBuffer();
    }, this.flushIntervalMs);
  }

  private stopFlushTimer(): void {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
  }

  private async flushBuffer(): Promise<void> {
    if (this.buffer.length === 0) {
      return;
    }

    const batch = this.buffer;
    this.buffer = [];

    try {
      await this.database.saveLiquidationEvents(batch);
      this.emit('liquidationDataSaved', batch.length);
    } catch (error) {
      logger.error('Failed to persist Binance liquidation data', error);
      this.emit('error', error as Error);
      this.buffer.unshift(...batch);
    }
  }
}
