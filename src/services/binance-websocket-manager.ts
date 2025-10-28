import WebSocket from 'ws';
import { EventEmitter } from 'events';
import { logger } from '../utils/logger';
import { MarketType, OHLCVData, WebSocketKlinePayload } from '../types';
import { SymbolMetadata } from '../types';

interface BinanceWebSocketManagerOptions {
  spotWsUrl: string;
  usdMWsUrl: string;
  coinMWsUrl: string;
  reconnectIntervalMs: number;
  maxSymbolsPerStream: number;
}

interface StreamConnection {
  market: MarketType;
  symbols: string[];
  ws: WebSocket | null;
  attempts: number;
  baseUrl: string;
  timer: NodeJS.Timeout | null;
}

export declare interface BinanceWebSocketManager {
  on(event: 'ohlcv', listener: (data: OHLCVData) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  on(event: 'reconnected', listener: (market: MarketType, symbols: string[]) => void): this;
}

export class BinanceWebSocketManager extends EventEmitter {
  private readonly connections: StreamConnection[] = [];
  private shouldRun = false;

  constructor(private readonly options: BinanceWebSocketManagerOptions) {
    super();
  }

  async start(symbolMap: Record<MarketType, SymbolMetadata[]>): Promise<void> {
    this.stop();
    this.shouldRun = true;

    this.createConnections('SPOT', symbolMap['SPOT'], this.options.spotWsUrl);
    this.createConnections('USDT-M', symbolMap['USDT-M'], this.options.usdMWsUrl);
    this.createConnections('COIN-M', symbolMap['COIN-M'], this.options.coinMWsUrl);

    for (const connection of this.connections) {
      this.openConnection(connection);
    }
  }

  stop(): void {
    this.shouldRun = false;
    for (const connection of this.connections) {
      if (connection.timer) {
        clearTimeout(connection.timer);
        connection.timer = null;
      }
      if (connection.ws) {
        connection.ws.removeAllListeners();
        connection.ws.close();
        connection.ws = null;
      }
    }
    this.connections.length = 0;
  }

  private createConnections(
    market: MarketType,
    symbols: SymbolMetadata[] | undefined,
    baseUrl: string
  ): void {
    if (!symbols || symbols.length === 0) {
      return;
    }

    const lowerSymbols = symbols.map((symbol) => symbol.symbol.toLowerCase());
    for (let i = 0; i < lowerSymbols.length; i += this.options.maxSymbolsPerStream) {
      const chunk = lowerSymbols.slice(i, i + this.options.maxSymbolsPerStream);
      this.connections.push({
        market,
        symbols: chunk,
        ws: null,
        attempts: 0,
        baseUrl,
        timer: null,
      });
    }
  }

  private openConnection(connection: StreamConnection): void {
    if (!this.shouldRun) {
      return;
    }

    connection.attempts += 1;
    const streamParams = connection.symbols.map((symbol) => `${symbol}@kline_1m`);
    const ws = new WebSocket(connection.baseUrl);
    connection.ws = ws;

    ws.on('open', () => {
      connection.attempts = 0;
      this.subscribe(ws, streamParams);
      logger.info('WebSocket connected', { market: connection.market, streams: streamParams.length });
    });

    ws.on('message', (message: WebSocket.RawData) => {
      this.handleMessage(connection.market, message.toString());
    });

    ws.on('error', (error) => {
      logger.error('WebSocket error', error);
      this.emit('error', error as Error);
    });

    ws.on('close', () => {
      if (!this.shouldRun) {
        return;
      }
      const delay = this.calculateBackoffDelay(connection.attempts);
      logger.warn('WebSocket disconnected, scheduling reconnect', {
        market: connection.market,
        delayMs: delay,
      });
      connection.timer = setTimeout(() => {
        this.openConnection(connection);
        this.emit('reconnected', connection.market, connection.symbols);
      }, delay);
    });
  }

  private subscribe(ws: WebSocket, params: string[]): void {
    const payload = {
      method: 'SUBSCRIBE',
      params,
      id: Date.now(),
    };
    ws.send(JSON.stringify(payload));
  }

  private handleMessage(market: MarketType, data: string): void {
    try {
      const parsed = JSON.parse(data) as WebSocketKlinePayload;
      if (!parsed?.data?.k) {
        return;
      }
      const kline = parsed.data.k;
      const ohlcv: OHLCVData = {
        symbol: kline.s,
        interval: '1m',
        openTime: kline.t,
        closeTime: kline.T,
        open: Number(kline.o),
        high: Number(kline.h),
        low: Number(kline.l),
        close: Number(kline.c),
        volume: Number(kline.v),
        quoteVolume: Number(kline.q),
        trades: kline.n,
      };
      this.emit('ohlcv', ohlcv);
    } catch (error) {
      logger.error(`Failed to parse WebSocket message for market ${market}`, error);
    }
  }

  private calculateBackoffDelay(attempt: number): number {
    const base = this.options.reconnectIntervalMs * Math.pow(2, attempt - 1);
    const jitter = Math.random() * this.options.reconnectIntervalMs;
    return Math.min(60_000, base + jitter);
  }
}
