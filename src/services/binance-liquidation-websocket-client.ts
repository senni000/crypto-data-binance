import { EventEmitter } from 'events';
import WebSocket from 'ws';
import { MarketType } from '../types';
import { logger } from '../utils/logger';

export interface BinanceLiquidationSubscription {
  symbol: string;
  marketType: Extract<MarketType, 'USDT-M' | 'COIN-M'>;
}

export interface BinanceLiquidationWebSocketClientOptions {
  usdMUrl: string;
  coinMUrl: string;
  subscriptions: BinanceLiquidationSubscription[];
  reconnectDelayMs?: number;
  heartbeatIntervalMs?: number;
}

export interface BinanceForceOrderEvent {
  marketType: Extract<MarketType, 'USDT-M' | 'COIN-M'>;
  symbol: string;
  eventTime: number;
  tradeTime?: number;
  side: 'BUY' | 'SELL';
  orderType?: string;
  timeInForce?: string;
  status?: string;
  price: number;
  averagePrice?: number;
  lastFilledPrice?: number;
  originalQuantity: number;
  filledQuantity: number;
  lastFilledQuantity?: number;
  isMaker?: boolean;
  reduceOnly?: boolean;
  orderId?: string;
}

interface MarketConnection {
  marketType: Extract<MarketType, 'USDT-M' | 'COIN-M'>;
  baseUrl: string;
  subscriptions: BinanceLiquidationSubscription[];
  ws: WebSocket | null;
  reconnectTimer: NodeJS.Timeout | null;
  heartbeatTimer: NodeJS.Timeout | null;
  ready: boolean;
}

export declare interface BinanceLiquidationWebSocketClient {
  on(event: 'liquidation', listener: (payload: BinanceForceOrderEvent) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  on(event: 'connected', listener: (market: MarketType) => void): this;
  on(event: 'disconnected', listener: (market: MarketType, code: number, reason: string) => void): this;
}

const DEFAULT_RECONNECT_DELAY_MS = 5_000;
const DEFAULT_HEARTBEAT_INTERVAL_MS = 30_000;

export class BinanceLiquidationWebSocketClient extends EventEmitter {
  private readonly connections: MarketConnection[];
  private readonly reconnectDelayMs: number;
  private readonly heartbeatIntervalMs: number;

  constructor(options: BinanceLiquidationWebSocketClientOptions) {
    super();

    this.reconnectDelayMs = options.reconnectDelayMs ?? DEFAULT_RECONNECT_DELAY_MS;
    this.heartbeatIntervalMs = options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS;
    this.connections = this.groupSubscriptions(options);
  }

  async connect(): Promise<void> {
    const tasks = this.connections
      .filter((connection) => connection.subscriptions.length > 0)
      .map((connection) => this.connectMarket(connection));

    await Promise.all(tasks);
  }

  disconnect(): void {
    for (const connection of this.connections) {
      this.clearTimers(connection);
      if (connection.ws) {
        connection.ws.removeAllListeners();
        connection.ws.close();
        connection.ws = null;
      }
      connection.ready = false;
    }
  }

  private groupSubscriptions(
    options: BinanceLiquidationWebSocketClientOptions
  ): MarketConnection[] {
    const byMarket: Record<'USDT-M' | 'COIN-M', MarketConnection> = {
      'USDT-M': {
        marketType: 'USDT-M',
        baseUrl: options.usdMUrl,
        subscriptions: [],
        ws: null,
        reconnectTimer: null,
        heartbeatTimer: null,
        ready: false,
      },
      'COIN-M': {
        marketType: 'COIN-M',
        baseUrl: options.coinMUrl,
        subscriptions: [],
        ws: null,
        reconnectTimer: null,
        heartbeatTimer: null,
        ready: false,
      },
    };

    for (const subscription of options.subscriptions) {
      const bucket = byMarket[subscription.marketType];
      if (!bucket) {
        continue;
      }

      const exists = bucket.subscriptions.some(
        (item) => item.symbol === subscription.symbol && item.marketType === subscription.marketType
      );

      if (!exists) {
        bucket.subscriptions.push(subscription);
      }
    }

    return Object.values(byMarket);
  }

  private connectMarket(connection: MarketConnection): Promise<void> {
    if (connection.subscriptions.length === 0) {
      return Promise.resolve();
    }

    const url = this.buildUrl(connection);

    return new Promise((resolve, reject) => {
      const ws = new WebSocket(url);
      connection.ws = ws;
      connection.ready = false;

      const onOpen = (): void => {
        connection.ready = true;
        this.startHeartbeat(connection);
        this.emit('connected', connection.marketType);
        logger.info(`Binance liquidation WS connected (${connection.marketType})`, { url });
        resolve();
      };

      const onError = (error: Error): void => {
        this.emit('error', error);
        if (!connection.ready) {
          reject(error);
        } else {
          logger.error(`Binance liquidation WS error (${connection.marketType})`, error);
        }
      };

      const onClose = (code: number, reasonBuffer: Buffer): void => {
        const reason = reasonBuffer.toString() || 'unknown';
        this.emit('disconnected', connection.marketType, code, reason);
        logger.warn(`Binance liquidation WS disconnected (${connection.marketType})`, {
          code,
          reason,
        });
        this.clearTimers(connection);
        connection.ready = false;
        if (code !== 1000) {
          this.scheduleReconnect(connection);
        }
      };

      const onMessage = (data: WebSocket.RawData): void => {
        this.handleMessage(connection, data);
      };

      ws.on('open', onOpen);
      ws.on('error', onError);
      ws.on('close', onClose);
      ws.on('message', onMessage);
    });
  }

  private buildUrl(connection: MarketConnection): string {
    const streams = connection.subscriptions.map((sub) => `${sub.symbol.toLowerCase()}@forceOrder`);
    const streamParam = streams.join('/');
    if (connection.baseUrl.includes('?')) {
      return `${connection.baseUrl}&streams=${streamParam}`;
    }
    return `${connection.baseUrl}?streams=${streamParam}`;
  }

  private handleMessage(connection: MarketConnection, payload: WebSocket.RawData): void {
    let parsed: any;
    try {
      parsed = JSON.parse(payload.toString());
    } catch (error) {
      logger.warn('Failed to parse Binance liquidation WS message', error);
      return;
    }

    const data = parsed.data ?? parsed;
    if (!data || typeof data !== 'object') {
      return;
    }

    const eventType = (data.e ?? data.eventType) as string | undefined;
    if (eventType !== 'forceOrder') {
      return;
    }

    const inner = data.o ?? data.order;
    if (!inner || typeof inner !== 'object') {
      return;
    }

    const symbol = typeof inner.s === 'string' ? inner.s : typeof data.s === 'string' ? data.s : undefined;
    if (!symbol) {
      return;
    }

    const sideRaw = typeof inner.S === 'string' ? inner.S.toUpperCase() : undefined;
    if (sideRaw !== 'BUY' && sideRaw !== 'SELL') {
      return;
    }

    const toNumber = (value: unknown): number | undefined => {
      if (value === undefined || value === null || value === '') {
        return undefined;
      }
      const num = Number(value);
      return Number.isFinite(num) ? num : undefined;
    };

    const eventTime = toNumber(data.E) ?? toNumber(parsed.E);
    const tradeTime = toNumber(inner.T);
    const originalQuantity = toNumber(inner.q) ?? toNumber(inner.Q);
    const filledQuantity = toNumber(inner.z) ?? toNumber(inner.l) ?? toNumber(inner.q);

    if (eventTime === undefined || originalQuantity === undefined || filledQuantity === undefined) {
      return;
    }

    const price = toNumber(inner.p) ?? toNumber(inner.L) ?? toNumber(inner.ap) ?? 0;
    const baseEvent: BinanceForceOrderEvent = {
      marketType: connection.marketType,
      symbol,
      eventTime,
      side: sideRaw,
      price,
      originalQuantity,
      filledQuantity,
    };

    if (tradeTime !== undefined) {
      baseEvent.tradeTime = tradeTime;
    }
    const orderType = typeof inner.o === 'string' ? inner.o : undefined;
    if (orderType !== undefined) {
      baseEvent.orderType = orderType;
    }
    const timeInForce = typeof inner.f === 'string' ? inner.f : undefined;
    if (timeInForce !== undefined) {
      baseEvent.timeInForce = timeInForce;
    }
    const status = typeof inner.X === 'string' ? inner.X : undefined;
    if (status !== undefined) {
      baseEvent.status = status;
    }
    const averagePrice = toNumber(inner.ap);
    if (averagePrice !== undefined) {
      baseEvent.averagePrice = averagePrice;
    }
    const lastFilledPrice = toNumber(inner.L);
    if (lastFilledPrice !== undefined) {
      baseEvent.lastFilledPrice = lastFilledPrice;
    }
    const lastFilledQuantity = toNumber(inner.l);
    if (lastFilledQuantity !== undefined) {
      baseEvent.lastFilledQuantity = lastFilledQuantity;
    }
    const isMaker = typeof inner.m === 'boolean' ? inner.m : undefined;
    if (isMaker !== undefined) {
      baseEvent.isMaker = isMaker;
    }
    const reduceOnly = typeof inner.R === 'boolean' ? inner.R : undefined;
    if (reduceOnly !== undefined) {
      baseEvent.reduceOnly = reduceOnly;
    }
    const orderId =
      inner.i !== undefined
        ? String(inner.i)
        : inner.O !== undefined
          ? String(inner.O)
          : undefined;
    if (orderId !== undefined) {
      baseEvent.orderId = orderId;
    }
    this.emit('liquidation', baseEvent);

  }

  private scheduleReconnect(connection: MarketConnection): void {
    if (connection.reconnectTimer) {
      return;
    }

    connection.reconnectTimer = setTimeout(() => {
      connection.reconnectTimer = null;
      logger.info(`Reconnecting Binance liquidation WS (${connection.marketType})`);
      void this.connectMarket(connection).catch((error) => {
        logger.error(`Failed to reconnect Binance liquidation WS (${connection.marketType})`, error);
        this.scheduleReconnect(connection);
      });
    }, this.reconnectDelayMs);
  }

  private startHeartbeat(connection: MarketConnection): void {
    this.clearHeartbeat(connection);
    if (!connection.ws) {
      return;
    }

    connection.heartbeatTimer = setInterval(() => {
      if (!connection.ws || connection.ws.readyState !== WebSocket.OPEN) {
        return;
      }
      try {
        connection.ws.ping();
      } catch (error) {
        logger.warn(`Failed to send liquidation WS ping (${connection.marketType})`, error as Error);
      }
    }, this.heartbeatIntervalMs);
  }

  private clearTimers(connection: MarketConnection): void {
    if (connection.reconnectTimer) {
      clearTimeout(connection.reconnectTimer);
      connection.reconnectTimer = null;
    }
    this.clearHeartbeat(connection);
  }

  private clearHeartbeat(connection: MarketConnection): void {
    if (connection.heartbeatTimer) {
      clearInterval(connection.heartbeatTimer);
      connection.heartbeatTimer = null;
    }
  }
}
