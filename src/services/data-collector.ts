import { EventEmitter } from 'events';
import { logger } from '../utils/logger';
import {
  IDataCollector,
  IDatabaseManager,
} from './interfaces';
import { BinanceRestClient } from './binance-rest-client';
import { SymbolManager } from './symbol-manager';
import { TopTraderAccountData, TopTraderPositionData } from '../types';

interface DataCollectorOptions {
  topTraderIntervalMs: number;
  topTraderRequestDelayMs?: number;
  topTraderMaxRetries?: number;
  topTraderRetryDelayMs?: number;
}

const DEFAULT_TOP_TRADER_REQUEST_DELAY_MS = 3_000;
const DEFAULT_TOP_TRADER_MAX_RETRIES = 3;
const DEFAULT_TOP_TRADER_RETRY_DELAY_MS = 5_000;

export declare interface DataCollector {
  on(event: 'restError', listener: (error: Error) => void): this;
  on(event: 'topTraderStored', listener: (payload: { positions: number; accounts: number }) => void): this;
}

export class DataCollector extends EventEmitter implements IDataCollector {
  private topTraderTimer: NodeJS.Timeout | null = null;
  private topTraderJob: Promise<void> | null = null;
  private isRunning = false;

  constructor(
    private readonly db: IDatabaseManager,
    private readonly symbolManager: SymbolManager,
    private readonly restClient: BinanceRestClient,
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

    this.symbolManager.on('updated', () => {
      logger.info('Symbol list updated');
    });
    this.symbolManager.on('error', (error) => this.emit('restError', error));

    await this.symbolManager.updateSymbols();
    this.symbolManager.scheduleDailyUpdate();

    this.scheduleTopTraderCollection();

    logger.info('Binance data collector started');
  }

  async stop(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    this.isRunning = false;
    if (this.topTraderTimer) {
      clearInterval(this.topTraderTimer);
      this.topTraderTimer = null;
    }

    if (this.topTraderJob) {
      try {
        await this.topTraderJob;
      } catch (error) {
        logger.warn('Top trader collection failed during shutdown', error);
      } finally {
        this.topTraderJob = null;
      }
    }

    logger.info('Binance data collector stopped');
  }

  private scheduleTopTraderCollection(): void {
    const launch = () => {
      if (this.topTraderJob) {
        logger.warn('Previous top trader collection still running, skipping this cycle');
        return;
      }

      this.topTraderJob = this.collectTopTraderData()
        .catch((error) => {
          logger.error('Failed to collect top trader data', error);
          this.emit('restError', error as Error);
        })
        .finally(() => {
          this.topTraderJob = null;
        });
    };

    launch();
    this.topTraderTimer = setInterval(launch, this.options.topTraderIntervalMs);
  }

  private async collectTopTraderData(): Promise<void> {
    const symbols = await this.symbolManager.getActiveSymbolsByMarket('USDT-M');
    if (symbols.length === 0) {
      return;
    }

    const eligibleSymbols = symbols.filter(
      (symbol) => !symbol.contractType || symbol.contractType === 'PERPETUAL'
    );

    let positionsCount = 0;
    let accountsCount = 0;

    const delayMs = this.options.topTraderRequestDelayMs ?? DEFAULT_TOP_TRADER_REQUEST_DELAY_MS;
    const maxRetries = this.options.topTraderMaxRetries ?? DEFAULT_TOP_TRADER_MAX_RETRIES;
    const retryDelayMs = this.options.topTraderRetryDelayMs ?? DEFAULT_TOP_TRADER_RETRY_DELAY_MS;

    for (let index = 0; index < eligibleSymbols.length && this.isRunning; index += 1) {
      const symbol = eligibleSymbols[index]!;

      if (index > 0 && delayMs > 0) {
        await this.delay(delayMs);
      }

      try {
        const positions = await this.fetchWithRetry(
          () => this.restClient.fetchTopTraderPositions(symbol.symbol),
          maxRetries,
          retryDelayMs
        );
        const accounts = await this.fetchWithRetry(
          () => this.restClient.fetchTopTraderAccounts(symbol.symbol),
          maxRetries,
          retryDelayMs
        );
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

  private async fetchWithRetry<T>(
    task: () => Promise<T>,
    maxRetries: number,
    retryDelayMs: number
  ): Promise<T> {
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
        logger.warn('Top trader request failed, retrying', {
          attempt,
          maxRetries,
          error: (error as Error).message,
        });
        await this.delay(retryDelayMs);
      }
    }

    throw lastError ?? new Error('Top trader request failed');
  }

  private async delay(ms: number): Promise<void> {
    if (ms <= 0) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, ms));
  }
}
