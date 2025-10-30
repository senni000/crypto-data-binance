import { EventEmitter } from 'events';
import WebSocket from 'ws';
import { TradeData, CvdStreamConfig, MarketType } from '../types';
import { logger } from '../utils/logger';

type StreamType = NonNullable<CvdStreamConfig['streamType']>;

interface SubscriptionEntry {
  symbol: string;
  streamType: StreamType;
}

interface MarketConnection {
  marketType: MarketType;
  baseUrl: string;
  subscriptions: SubscriptionEntry[];
  ws: WebSocket | null;
  reconnectTimer: NodeJS.Timeout | null;
  heartbeatTimer: NodeJS.Timeout | null;
  ready: boolean;
}

export interface BinanceTradeWebSocketClientOptions {
  spotUrl: string;
  usdMUrl: string;
  coinMUrl: string;
  subscriptions: Array<{
    symbol: string;
    marketType: MarketType;
    streamType: StreamType;
  }>;
  reconnectDelayMs?: number;
  heartbeatIntervalMs?: number;
}

export declare interface BinanceTradeWebSocketClient {
  on(event: 'trade', listener: (trade: TradeData) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  on(event: 'connected', listener: (market: MarketType) => void): this;
  on(event: 'disconnected', listener: (market: MarketType, code: number, reason: string) => void): this;
}

const DEFAULT_RECONNECT_DELAY_MS = 5_000;
const DEFAULT_HEARTBEAT_INTERVAL_MS = 30_000;

/**
 * Binance の複数市場向けトレード WebSocket クライアント
 */
export class BinanceTradeWebSocketClient extends EventEmitter {
  private readonly connections: MarketConnection[];
  private readonly reconnectDelayMs: number;
  private readonly heartbeatIntervalMs: number;

  constructor(options: BinanceTradeWebSocketClientOptions) {
    super();

    const grouped = this.groupSubscriptions(options);
    this.connections = grouped;
    this.reconnectDelayMs = options.reconnectDelayMs ?? DEFAULT_RECONNECT_DELAY_MS;
    this.heartbeatIntervalMs = options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS;
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

  private groupSubscriptions(options: BinanceTradeWebSocketClientOptions): MarketConnection[] {
    const byMarket: Record<MarketType, MarketConnection> = {
      'SPOT': {
        marketType: 'SPOT',
        baseUrl: options.spotUrl,
        subscriptions: [],
        ws: null,
        reconnectTimer: null,
        heartbeatTimer: null,
        ready: false,
      },
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
      const market = byMarket[subscription.marketType];
      if (!market) {
        logger.warn('Unsupported market for WebSocket subscription', subscription);
        continue;
      }

      const exists = market.subscriptions.some(
        (item) =>
          item.symbol === subscription.symbol && item.streamType === subscription.streamType
      );

      if (!exists) {
        market.subscriptions.push({
          symbol: subscription.symbol,
          streamType: subscription.streamType,
        });
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
        logger.info(`Binance WS connected (${connection.marketType})`, { url });
        resolve();
      };

      const onError = (error: Error): void => {
        this.emit('error', error);
        if (!connection.ready) {
          reject(error);
        } else {
          logger.error(`Binance WS error (${connection.marketType})`, error);
        }
      };

      const onClose = (code: number, reasonBuffer: Buffer): void => {
        const reason = reasonBuffer.toString() || 'unknown';
        this.emit('disconnected', connection.marketType, code, reason);
        logger.warn(`Binance WS disconnected (${connection.marketType})`, { code, reason });
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

  private scheduleReconnect(connection: MarketConnection): void {
    if (connection.reconnectTimer) {
      return;
    }

    connection.reconnectTimer = setTimeout(() => {
      connection.reconnectTimer = null;
      logger.info(`Reconnecting Binance WS (${connection.marketType})`);
      void this.connectMarket(connection).catch((error) => {
        logger.error(`Failed to reconnect Binance WS (${connection.marketType})`, error);
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
        logger.warn(`Failed to send WS ping (${connection.marketType})`, error as Error);
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

  private buildUrl(connection: MarketConnection): string {
    const streams = connection.subscriptions.map((sub) => {
      const symbol = sub.symbol.toLowerCase();
      const channel = sub.streamType === 'trade' ? 'trade' : 'aggTrade';
      return `${symbol}@${channel}`;
    });

    const query = streams.join('/');
    if (connection.baseUrl.includes('?')) {
      return `${connection.baseUrl}&streams=${query}`;
    }
    return `${connection.baseUrl}?streams=${query}`;
  }

  private handleMessage(connection: MarketConnection, payload: WebSocket.RawData): void {
    let parsed: any;
    try {
      parsed = JSON.parse(payload.toString());
    } catch (error) {
      logger.warn('Failed to parse Binance WS message', error);
      return;
    }

    const data = parsed.data ?? parsed;
    if (!data || typeof data !== 'object') {
      return;
    }

    const eventType = data.e as string | undefined;
    if (eventType !== 'aggTrade' && eventType !== 'trade') {
      return;
    }

    const symbol = typeof data.s === 'string' ? data.s : undefined;
    if (!symbol) {
      return;
    }

    const tradeIdValue =
      eventType === 'aggTrade'
        ? data.a ?? data.A
        : data.t ?? data.T ?? data.a ?? data.A;

    const priceValue = data.p ?? data.price;
    const quantityValue = data.q ?? data.quantity;
    const timeValue = data.T ?? data.E ?? data.eventTime;

    const price = Number(priceValue);
    const quantity = Number(quantityValue);
    const timestamp = Number(timeValue);
    const tradeId = tradeIdValue !== undefined ? String(tradeIdValue) : `${symbol}-${timestamp}`;

    if (!Number.isFinite(price) || !Number.isFinite(quantity) || !Number.isFinite(timestamp)) {
      return;
    }

    const direction = data.m ? 'sell' : 'buy';

    const trade: TradeData = {
      symbol,
      marketType: connection.marketType,
      streamType: eventType,
      tradeId,
      price,
      amount: quantity,
      timestamp,
      direction,
    };

    this.emit('trade', trade);
  }
}
