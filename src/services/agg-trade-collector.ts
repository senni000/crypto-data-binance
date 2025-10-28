import { EventEmitter } from 'events';
import { logger } from '../utils/logger';
import { loadCoinMarketCapTopEntries } from '../utils/coinmarketcap';
import { AggTrade } from '../types';
import { IAggTradeDatabaseManager } from './interfaces';
import { SymbolManager } from './symbol-manager';
import { BinanceRestClient } from './binance-rest-client';

const DEFAULT_FETCH_INTERVAL_MS = 60 * 60 * 1000; // 1 hour
const DEFAULT_INITIAL_LOOKBACK_MS = 12 * 60 * 60 * 1000; // 12 hours
const MAX_REST_ITERATIONS = 50;
const DEFAULT_REST_LIMIT = 1_000;
const DEFAULT_MAX_RETRIES = 3;
const DEFAULT_RETRY_DELAY_MS = 5_000;
const REQUEST_COOLDOWN_MS = 500;

interface AggTradeCollectorOptions {
  assetListPath: string;
  fetchIntervalMs?: number;
  restLimit?: number;
  initialLookbackMs?: number;
  maxRetries?: number;
  retryDelayMs?: number;
}

interface TargetPair {
  asset: string;
  symbol: string;
  marketType: AggTrade['marketType'];
}

export declare interface AggTradeCollector {
  on(event: 'tradeStored', listener: (count: number) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
}

export class AggTradeCollector extends EventEmitter {
  private isRunning = false;
  private timer: NodeJS.Timeout | null = null;
  private currentCycle: Promise<void> | null = null;
  private targets: TargetPair[] = [];

  constructor(
    private readonly db: IAggTradeDatabaseManager,
    private readonly symbolManager: SymbolManager,
    private readonly restClient: BinanceRestClient,
    private readonly options: AggTradeCollectorOptions
  ) {
    super();
  }

  async start(): Promise<void> {
    if (this.isRunning) {
      return;
    }
    this.isRunning = true;

    await this.db.initialize();

    let assets: Array<{ symbol: string }>;
    try {
      assets = loadCoinMarketCapTopEntries(this.options.assetListPath, { limit: 100 });
    } catch (error) {
      logger.error('Failed to load CoinMarketCap asset list', error);
      this.emit('error', error as Error);
      this.isRunning = false;
      return;
    }

    this.targets = await this.resolveTargetPairs(assets);
    if (this.targets.length === 0) {
      logger.warn('No target pairs resolved from CoinMarketCap list');
      this.isRunning = false;
      return;
    }

    await this.runCycle('initial');
    if (!this.isRunning) {
      return;
    }
    this.scheduleNext();

    logger.info('AggTrade collector started', {
      pairs: this.targets.length,
      intervalMs: this.fetchIntervalMs,
    });
  }

  async stop(): Promise<void> {
    if (!this.isRunning) {
      return;
    }
    this.isRunning = false;

    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    const cycle = this.currentCycle;
    if (cycle) {
      try {
        await cycle;
      } catch (error) {
        logger.warn('AggTrade cycle failed during shutdown', error);
      }
    }

    await this.db.close();
    logger.info('AggTrade collector stopped');
  }

  private get fetchIntervalMs(): number {
    return this.options.fetchIntervalMs ?? DEFAULT_FETCH_INTERVAL_MS;
  }

  private get restLimit(): number {
    return this.options.restLimit ?? DEFAULT_REST_LIMIT;
  }

  private get initialLookbackMs(): number {
    return this.options.initialLookbackMs ?? DEFAULT_INITIAL_LOOKBACK_MS;
  }

  private get maxRetries(): number {
    return this.options.maxRetries ?? DEFAULT_MAX_RETRIES;
  }

  private get retryDelayMs(): number {
    return this.options.retryDelayMs ?? DEFAULT_RETRY_DELAY_MS;
  }

  private scheduleNext(): void {
    if (!this.isRunning) {
      return;
    }

    this.timer = setTimeout(() => {
      void this.runCycle('scheduled').finally(() => {
        this.scheduleNext();
      });
    }, this.fetchIntervalMs);
  }

  private async runCycle(mode: 'initial' | 'scheduled'): Promise<void> {
    const cycle = this.performFetch(mode);
    this.currentCycle = cycle;
    try {
      await cycle;
    } finally {
      if (this.currentCycle === cycle) {
        this.currentCycle = null;
      }
    }
  }

  private async performFetch(mode: 'initial' | 'scheduled'): Promise<void> {
    let storedTotal = 0;

    for (const pair of this.targets) {
      if (!this.isRunning) {
        break;
      }

      const checkpoint = await this.db.getLastAggTradeCheckpoint(
        pair.asset,
        pair.symbol,
        pair.marketType
      );

      let cursor =
        checkpoint?.tradeTime !== undefined
          ? checkpoint.tradeTime + 1
          : Date.now() - this.initialLookbackMs;

      if (mode === 'scheduled') {
        const minimumStart = Date.now() - this.fetchIntervalMs;
        cursor = Math.max(cursor, minimumStart);
      }

      let iterations = 0;

      while (this.isRunning && iterations < MAX_REST_ITERATIONS) {
        iterations += 1;

        const params: {
          startTime?: number;
          limit?: number;
        } = {
          startTime: cursor,
          limit: this.restLimit,
        };

        let trades: AggTrade[] = [];

        try {
          trades = await this.fetchAggTradesWithRetry(
            () => this.restClient.fetchAggTrades(pair.symbol, pair.marketType, params),
            this.maxRetries,
            this.retryDelayMs
          );
        } catch (error) {
          logger.error('Failed to fetch agg trades', {
            pair,
            error: (error as Error).message,
          });
          this.emit('error', error as Error);
          break;
        }

        if (trades.length === 0) {
          break;
        }

        try {
          await this.db.saveAggTrades(pair.asset, trades);
          storedTotal += trades.length;
        } catch (error) {
          logger.error('Failed to persist agg trades', {
            pair,
            error: (error as Error).message,
          });
          this.emit('error', error as Error);
          break;
        }

        const lastTrade = trades[trades.length - 1]!;
        cursor = lastTrade.tradeTime + 1;

        if (trades.length < this.restLimit) {
          break;
        }

        await this.delay(REQUEST_COOLDOWN_MS);
      }
    }

    if (storedTotal > 0) {
      this.emit('tradeStored', storedTotal);
    }
  }

  private async resolveTargetPairs(assets: { symbol: string }[]): Promise<TargetPair[]> {
    const results: TargetPair[] = [];
    const spotSymbols = await this.symbolManager.getActiveSymbolsByMarket('SPOT');
    const futuresSymbols = await this.symbolManager.getActiveSymbolsByMarket('USDT-M');

    const spotMap = new Map(
      spotSymbols
        .filter((symbol) => symbol.quoteAsset === 'USDT')
        .map((symbol) => [symbol.baseAsset.toUpperCase(), symbol.symbol])
    );
    const futuresMap = new Map(
      futuresSymbols
        .filter((symbol) => !symbol.contractType || symbol.contractType === 'PERPETUAL')
        .map((symbol) => [symbol.baseAsset.toUpperCase(), symbol.symbol])
    );

    for (const asset of assets) {
      const base = asset.symbol.toUpperCase();

      const spotSymbol = spotMap.get(base);
      if (spotSymbol) {
        results.push({
          asset: base,
          symbol: spotSymbol,
          marketType: 'SPOT',
        });
      }

      const perpSymbol = futuresMap.get(base);
      if (perpSymbol) {
        results.push({
          asset: base,
          symbol: perpSymbol,
          marketType: 'USDT-M',
        });
      }
    }

    return results;
  }

  private async fetchAggTradesWithRetry(
    task: () => Promise<AggTrade[]>,
    maxRetries: number,
    retryDelayMs: number
  ): Promise<AggTrade[]> {
    let attempt = 0;
    let lastError: unknown;

    while (attempt < maxRetries && this.isRunning) {
      attempt += 1;
      try {
        return await task();
      } catch (error) {
        lastError = error;
        if (attempt >= maxRetries) {
          break;
        }
        logger.warn('AggTrade REST request failed, retrying', {
          attempt,
          maxRetries,
          error: (error as Error).message,
        });
        await this.delay(retryDelayMs);
      }
    }

    throw lastError ?? new Error('AggTrade REST request failed');
  }

  private async delay(ms: number): Promise<void> {
    if (ms <= 0) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, ms));
  }
}
