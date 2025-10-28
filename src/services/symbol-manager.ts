import axios, { AxiosInstance } from 'axios';
import { EventEmitter } from 'events';
import { logger } from '../utils/logger';
import {
  IDatabaseManager,
  ISymbolManager,
  IRateLimiter,
  RateLimiterRequest,
} from './interfaces';
import { MarketType, SymbolMetadata } from '../types';

interface SymbolManagerOptions {
  spotBaseUrl: string;
  usdMBaseUrl: string;
  coinMBaseUrl: string;
  symbolUpdateHourUtc: number;
}

type ExchangeInfoResponse = {
  symbols: Array<{
    symbol: string;
    status: string;
    baseAsset: string;
    quoteAsset: string;
    contractType?: string;
    deliveryDate?: number;
    onboardDate?: number;
  permissions?: string[];
  permissionSets?: Array<string | string[]>;
  isSpotTradingAllowed?: boolean;
    filters: Array<{
      filterType: string;
      tickSize?: string;
      stepSize?: string;
      minNotional?: string;
      notional?: string;
    }>;
  }>;
};

const SIX_HOURS_MS = 6 * 60 * 60 * 1000;

export declare interface SymbolManager {
  on(event: 'updated', listener: (symbols: SymbolMetadata[]) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
}

export class SymbolManager extends EventEmitter implements ISymbolManager {
  private readonly spotClient: AxiosInstance;
  private readonly usdMClient: AxiosInstance;
  private readonly coinMClient: AxiosInstance;
  private timer: NodeJS.Timeout | null = null;

  constructor(
    private readonly db: IDatabaseManager,
    private readonly rateLimiter: IRateLimiter,
    private readonly options: SymbolManagerOptions
  ) {
    super();
    this.spotClient = axios.create({ baseURL: options.spotBaseUrl, timeout: 10_000 });
    this.usdMClient = axios.create({ baseURL: options.usdMBaseUrl, timeout: 10_000 });
    this.coinMClient = axios.create({ baseURL: options.coinMBaseUrl, timeout: 10_000 });
  }

  scheduleDailyUpdate(): void {
    if (this.timer) {
      clearTimeout(this.timer);
    }

    const now = new Date();
    const nextRun = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), this.options.symbolUpdateHourUtc, 0, 0, 0));

    if (nextRun.getTime() <= now.getTime()) {
      nextRun.setUTCDate(nextRun.getUTCDate() + 1);
    }

    const delay = Math.max(nextRun.getTime() - now.getTime(), 5_000);
    logger.info('Scheduling next symbol update', { delayMs: delay });

    this.timer = setTimeout(() => {
      this.updateSymbols()
        .catch((error) => {
          logger.error('Failed to update symbols', error);
          // リトライ安全策: 6時間後に再スケジュール
          this.timer = setTimeout(() => this.scheduleDailyUpdate(), SIX_HOURS_MS);
        })
        .finally(() => {
          this.scheduleDailyUpdate();
        });
    }, delay);
  }

  async updateSymbols(): Promise<void> {
    logger.info('Updating Binance symbols...');

    const [spot, usdM, coinM] = await Promise.all([
      this.fetchSpotSymbols(),
      this.fetchUsdMSymbols(),
      this.fetchCoinMSymbols(),
    ]);

    const merged = [...spot, ...usdM, ...coinM];
    await this.db.upsertSymbols(merged);

    await this.deactivateRemovedSymbols(merged);

    logger.info('Symbol update completed', { totalSymbols: merged.length });
    this.emit('updated', merged);
  }

  async getActiveSymbolsByMarket(market: MarketType): Promise<SymbolMetadata[]> {
    return this.db.listActiveSymbols(market);
  }

  async getAllActiveSymbols(): Promise<SymbolMetadata[]> {
    return this.db.listActiveSymbols();
  }

  private async deactivateRemovedSymbols(latestSymbols: SymbolMetadata[]): Promise<void> {
    const known = await this.db.listAllSymbols();
    const latestSet = new Set(latestSymbols.map((s) => `${s.symbol}|${s.marketType}`));
    const toDeactivate = known
      .filter((symbol) => symbol.status === 'ACTIVE' && !latestSet.has(`${symbol.symbol}|${symbol.marketType}`))
      .map((symbol) => ({ symbol: symbol.symbol, marketType: symbol.marketType }));

    if (toDeactivate.length > 0) {
      logger.warn('Marking symbols inactive', { count: toDeactivate.length });
      await this.db.markSymbolsInactive(toDeactivate);
    }
  }

  private async fetchSpotSymbols(): Promise<SymbolMetadata[]> {
    const response = await this.requestWithRateLimit(
      { weight: 10, identifier: 'exchangeInfo:spot' },
      () => this.spotClient.get<ExchangeInfoResponse>('/api/v3/exchangeInfo')
    );
    return response.data.symbols
      .filter((symbol) => this.isSpotEligible(symbol))
      .map((symbol) => this.mapSymbol(symbol, 'SPOT'));
  }

  private async fetchUsdMSymbols(): Promise<SymbolMetadata[]> {
    const response = await this.requestWithRateLimit(
      { weight: 10, identifier: 'exchangeInfo:usdm' },
      () => this.usdMClient.get<ExchangeInfoResponse>('/fapi/v1/exchangeInfo')
    );
    return response.data.symbols.map((symbol) => this.mapSymbol(symbol, 'USDT-M'));
  }

  private async fetchCoinMSymbols(): Promise<SymbolMetadata[]> {
    const response = await this.requestWithRateLimit(
      { weight: 10, identifier: 'exchangeInfo:coinm' },
      () => this.coinMClient.get<ExchangeInfoResponse>('/dapi/v1/exchangeInfo')
    );
    return response.data.symbols.map((symbol) => this.mapSymbol(symbol, 'COIN-M'));
  }

  private mapSymbol(symbol: ExchangeInfoResponse['symbols'][number], market: MarketType): SymbolMetadata {
    const filters = this.extractFilters(symbol.filters);
    const status = symbol.status === 'TRADING' ? 'ACTIVE' : 'INACTIVE';
    const onboardDate =
      symbol.onboardDate !== undefined
        ? Number(symbol.onboardDate)
        : Date.now();

    const metadata: SymbolMetadata = {
      symbol: symbol.symbol,
      baseAsset: symbol.baseAsset,
      quoteAsset: symbol.quoteAsset,
      marketType: market,
      status,
      onboardDate,
      filters,
      updatedAt: Date.now(),
    };

    if (symbol.contractType !== undefined) {
      metadata.contractType = symbol.contractType;
    }

    if (symbol.deliveryDate !== undefined) {
      metadata.deliveryDate = symbol.deliveryDate;
    }

    return metadata;
  }

  private extractFilters(filters: ExchangeInfoResponse['symbols'][number]['filters']): SymbolMetadata['filters'] {
    const result: SymbolMetadata['filters'] = {};

    for (const filter of filters) {
      if (filter.filterType === 'PRICE_FILTER' && filter.tickSize) {
        result.tickSize = Number(filter.tickSize);
      }
      if (filter.filterType === 'LOT_SIZE' && filter.stepSize) {
        result.stepSize = Number(filter.stepSize);
      }
      if (filter.filterType === 'MIN_NOTIONAL') {
        const value = filter.minNotional ?? filter.notional;
        if (value) {
          result.minNotional = Number(value);
        }
      }
    }

    return result;
  }

  private isSpotEligible(symbol: ExchangeInfoResponse['symbols'][number]): boolean {
    if (Array.isArray(symbol.permissions) && symbol.permissions.includes('SPOT')) {
      return true;
    }
    if (Array.isArray(symbol.permissionSets)) {
      return symbol.permissionSets.some((set) => {
        if (Array.isArray(set)) {
          return set.includes('SPOT');
        }
        return set === 'SPOT';
      });
    }
    return Boolean((symbol as { isSpotTradingAllowed?: boolean }).isSpotTradingAllowed);
  }

  private async requestWithRateLimit<T>(
    request: RateLimiterRequest,
    task: () => Promise<T>
  ): Promise<T> {
    try {
      return await this.rateLimiter.schedule(request, task);
    } catch (error) {
      this.emit('error', error as Error);
      throw error;
    }
  }
}
