import axios, { AxiosInstance } from 'axios';
import { logger } from '../utils/logger';
import {
  IRateLimiter,
  RateLimiterRequest,
} from './interfaces';
import {
  MarketType,
  OHLCVData,
  OHLCVTimeframe,
  AggTrade,
  TopTraderAccountData,
  TopTraderPositionData,
} from '../types';

interface BinanceRestClientOptions {
  spotBaseUrl: string;
  usdMBaseUrl: string;
  coinMBaseUrl: string;
  timeout: number;
  defaultKlineLimit?: number;
  usedWeightThrottleThreshold?: number;
  usedWeightThrottleDelayMs?: number;
}

type KlineResponse = Array<
  [
    number,
    string,
    string,
    string,
    string,
    string,
    number,
    string,
    number,
    string,
    string,
    string
  ]
>;

type TopTraderRatioResponse = Array<{
  symbol: string;
  longShortRatio: string;
  longAccount: string;
  shortAccount: string;
  longPosition?: string;
  shortPosition?: string;
  timestamp: number;
}>;

type AggTradeResponse = Array<{
  a: number;
  p: string;
  q: string;
  f: number;
  l: number;
  T: number;
  m: boolean;
  M: boolean;
}>;

export class BinanceRestClient {
  private readonly spotClient: AxiosInstance;
  private readonly usdMClient: AxiosInstance;
  private readonly coinMClient: AxiosInstance;
  private readonly defaultKlineLimit: number;
  private readonly usedWeightThrottleThreshold: number;
  private readonly usedWeightThrottleDelayMs: number;
  private readonly usedWeightLimit = 1_200;

  constructor(
    private readonly rateLimiter: IRateLimiter,
    options: BinanceRestClientOptions
  ) {
    this.spotClient = axios.create({
      baseURL: options.spotBaseUrl,
      timeout: options.timeout,
    });
    this.usdMClient = axios.create({
      baseURL: options.usdMBaseUrl,
      timeout: options.timeout,
    });
    this.coinMClient = axios.create({
      baseURL: options.coinMBaseUrl,
      timeout: options.timeout,
    });
    this.defaultKlineLimit = options.defaultKlineLimit ?? 500;
    this.usedWeightThrottleThreshold = options.usedWeightThrottleThreshold ?? 1_000;
    this.usedWeightThrottleDelayMs = options.usedWeightThrottleDelayMs ?? 2_000;
  }

  async fetchKlines(
    symbol: string,
    interval: OHLCVTimeframe,
    market: MarketType,
    startTime?: number
  ): Promise<OHLCVData[]> {
    const client = this.resolveClient(market);
    const path = this.getKlinePath(market);
    const params: Record<string, string | number> = {
      symbol,
      interval: this.mapInterval(interval),
      limit: this.defaultKlineLimit,
    };
    if (startTime) {
      params['startTime'] = startTime;
    }

    const identifier = `klines:${market}`;
    const weight = 2;

    const response = await this.scheduleRequest(
      { identifier, weight },
      () => client.get<KlineResponse>(path, { params })
    );

    return response.data.map((row) => this.mapKlineRow(row, symbol, interval));
  }

  async fetchAggTrades(
    symbol: string,
    market: AggTrade['marketType'],
    options: {
      startTime?: number;
      endTime?: number;
      fromId?: number;
      limit?: number;
    } = {}
  ): Promise<AggTrade[]> {
    const client = this.resolveAggTradeClient(market);
    const path = this.getAggTradePath(market);
    const params: Record<string, string | number> = {
      symbol,
      limit: options.limit ?? 1000,
    };
    if (options.startTime !== undefined) {
      params['startTime'] = options.startTime;
    }
    if (options.endTime !== undefined) {
      params['endTime'] = options.endTime;
    }
    if (options.fromId !== undefined) {
      params['fromId'] = options.fromId;
    }
    const identifier = `aggTrades:${market}`;
    const weight = market === 'SPOT' ? 2 : 20;

    const response = await this.scheduleRequest(
      { identifier, weight },
      () => client.get<AggTradeResponse>(path, { params })
    );

    return response.data.map((row) =>
      this.mapAggTradeRow(symbol, market, row, 'rest')
    );
  }

  async fetchTopTraderPositions(symbol: string): Promise<TopTraderPositionData[]> {
    const response = await this.scheduleRequest(
      { identifier: 'topTrader:positions', weight: 20 },
      () =>
        this.usdMClient.get<TopTraderRatioResponse>('/futures/data/topLongShortPositionRatio', {
          params: {
            symbol,
            period: '5m',
            limit: 12,
          },
        })
    );

    return response.data.map((row) => ({
      symbol: row.symbol,
      timestamp: row.timestamp,
      longShortRatio: Number(row.longShortRatio),
      longAccount: Number(row.longAccount),
      shortAccount: Number(row.shortAccount),
      longPosition: Number(row.longPosition ?? 0),
      shortPosition: Number(row.shortPosition ?? 0),
    }));
  }

  async fetchTopTraderAccounts(symbol: string): Promise<TopTraderAccountData[]> {
    const response = await this.scheduleRequest(
      { identifier: 'topTrader:accounts', weight: 20 },
      () =>
        this.usdMClient.get<TopTraderRatioResponse>('/futures/data/topLongShortAccountRatio', {
          params: {
            symbol,
            period: '5m',
            limit: 12,
          },
        })
    );

    return response.data.map((row) => ({
      symbol: row.symbol,
      timestamp: row.timestamp,
      longShortRatio: Number(row.longShortRatio),
      longAccount: Number(row.longAccount),
      shortAccount: Number(row.shortAccount),
    }));
  }

  private resolveClient(market: MarketType): AxiosInstance {
    switch (market) {
      case 'SPOT':
        return this.spotClient;
      case 'USDT-M':
        return this.usdMClient;
      case 'COIN-M':
        return this.coinMClient;
      default:
        throw new Error(`Unsupported market type: ${market}`);
    }
  }

  private resolveAggTradeClient(market: AggTrade['marketType']): AxiosInstance {
    switch (market) {
      case 'SPOT':
        return this.spotClient;
      case 'USDT-M':
        return this.usdMClient;
      default:
        throw new Error(`Unsupported market type for aggTrades: ${market}`);
    }
  }

  private getKlinePath(market: MarketType): string {
    switch (market) {
      case 'SPOT':
        return '/api/v3/klines';
      case 'USDT-M':
        return '/fapi/v1/klines';
      case 'COIN-M':
        return '/dapi/v1/klines';
      default:
        throw new Error(`Unsupported market type: ${market}`);
    }
  }

  private getAggTradePath(market: AggTrade['marketType']): string {
    switch (market) {
      case 'SPOT':
        return '/api/v3/aggTrades';
      case 'USDT-M':
        return '/fapi/v1/aggTrades';
      default:
        throw new Error(`Unsupported market type for aggTrades: ${market}`);
    }
  }

  private mapInterval(interval: OHLCVTimeframe): string {
    switch (interval) {
      case '1m':
        return '1m';
      case '30m':
        return '30m';
      case '1d':
        return '1d';
      default:
        throw new Error(`Unsupported interval ${interval}`);
    }
  }

  private mapKlineRow(row: KlineResponse[number], symbol: string, interval: OHLCVTimeframe): OHLCVData {
    return {
      symbol,
      interval,
      openTime: row[0],
      closeTime: row[6],
      open: Number(row[1]),
      high: Number(row[2]),
      low: Number(row[3]),
      close: Number(row[4]),
      volume: Number(row[5]),
      quoteVolume: Number(row[7]),
      trades: row[8],
    };
  }

  private mapAggTradeRow(
    symbol: string,
    market: AggTrade['marketType'],
    row: AggTradeResponse[number],
    source: AggTrade['source']
  ): AggTrade {
    return {
      symbol,
      marketType: market,
      tradeId: row.a,
      price: Number(row.p),
      quantity: Number(row.q),
      firstTradeId: row.f,
      lastTradeId: row.l,
      tradeTime: row.T,
      isBuyerMaker: Boolean(row.m),
      isBestMatch: Boolean(row.M),
      source,
    };
  }

  private async scheduleRequest<T>(request: RateLimiterRequest, task: () => Promise<T>): Promise<T> {
    try {
      const response = await this.rateLimiter.schedule(request, async () => {
        const result = await task();
        await this.handleRateLimitHeaders(request.identifier ?? 'unknown', result, request.weight);
        return result;
      });
      return response;
    } catch (error) {
      logger.error('REST request failed', error);
      throw error;
    }
  }

  private async handleRateLimitHeaders(endpoint: string, response: unknown, weight: number): Promise<void> {
    if (!response || typeof response !== 'object') {
      return;
    }

    const headers = (response as { headers?: Record<string, unknown> }).headers;
    if (!headers) {
      return;
    }

    const raw =
      headers['x-mbx-used-weight-1m'] ??
      headers['X-MBX-USED-WEIGHT-1M'] ??
      headers['x-mbx-used-weight'] ??
      headers['X-MBX-USED-WEIGHT'];

    const usedWeight = this.parseNumericHeader(raw);
    if (usedWeight === undefined) {
      return;
    }

    logger.info('Binance REST weight usage', {
      endpoint,
      usedWeight1m: usedWeight,
      weight,
    });

    if (usedWeight < this.usedWeightThrottleThreshold) {
      return;
    }

    const overload = usedWeight - this.usedWeightThrottleThreshold;
    const ratio = Math.min(1, usedWeight / this.usedWeightLimit);
    const delay =
      this.usedWeightThrottleDelayMs +
      Math.max(0, overload) * (this.usedWeightThrottleDelayMs / Math.max(1, weight)) * ratio;

    logger.warn('Throttling due to Binance weight usage', {
      endpoint,
      usedWeight1m: usedWeight,
      delayMs: Math.round(delay),
    });

    await this.delay(delay);
  }

  private parseNumericHeader(value: unknown): number | undefined {
    if (value === undefined || value === null) {
      return undefined;
    }
    const raw = Array.isArray(value) ? value[0] : value;
    const parsed = Number(raw);
    return Number.isFinite(parsed) ? parsed : undefined;
  }

  private async delay(ms: number): Promise<void> {
    if (ms <= 0) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, ms));
  }
}
