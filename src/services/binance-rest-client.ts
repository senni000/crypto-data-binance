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
  TopTraderAccountData,
  TopTraderPositionData,
} from '../types';

interface BinanceRestClientOptions {
  spotBaseUrl: string;
  usdMBaseUrl: string;
  coinMBaseUrl: string;
  timeout: number;
  defaultKlineLimit?: number;
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

export class BinanceRestClient {
  private readonly spotClient: AxiosInstance;
  private readonly usdMClient: AxiosInstance;
  private readonly coinMClient: AxiosInstance;
  private readonly defaultKlineLimit: number;

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

  private async scheduleRequest<T>(request: RateLimiterRequest, task: () => Promise<T>): Promise<T> {
    try {
      return await this.rateLimiter.schedule(request, task);
    } catch (error) {
      logger.error('REST request failed', error);
      throw error;
    }
  }
}
