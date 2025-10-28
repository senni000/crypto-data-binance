/**
 * サービス層インターフェース定義
 */

import {
  MarketType,
  OHLCVData,
  OHLCVTimeframe,
  AggTrade,
  SymbolMetadata,
  TopTraderAccountData,
  TopTraderPositionData,
} from '../types';
import { LogLevel } from '../types/config';

export interface AppConfig {
  databasePath: string;
  databaseBackupEnabled: boolean;
  databaseBackupDirectory: string;
  databaseBackupInterval: number;
  databaseBackupSingleFile: boolean;
  logLevel: LogLevel;
  binanceRestBaseUrl: string;
  binanceUsdMRestBaseUrl: string;
  binanceCoinMRestBaseUrl: string;
  rateLimitBuffer: number;
  restRequestTimeout: number;
  symbolUpdateHourUtc: number;
  aggTradeDataDirectory: string;
}

export interface IDataCollector {
  start(): Promise<void>;
  stop(): Promise<void>;
}

export interface IDatabaseManager {
  initialize(): Promise<void>;
  runMigrations(): Promise<void>;
  upsertSymbols(symbols: SymbolMetadata[]): Promise<void>;
  listActiveSymbols(marketType?: MarketType): Promise<SymbolMetadata[]>;
  listAllSymbols(): Promise<SymbolMetadata[]>;
  markSymbolsInactive(entries: Array<{ symbol: string; marketType: MarketType }>): Promise<void>;
  saveOHLCVBatch(data: OHLCVData[]): Promise<void>;
  saveAggTrades(trades: AggTrade[]): Promise<void>;
  saveTopTraderPositions(data: TopTraderPositionData[]): Promise<void>;
  saveTopTraderAccounts(data: TopTraderAccountData[]): Promise<void>;
  pruneDataBefore(timeframe: OHLCVTimeframe, cutoff: number): Promise<void>;
  pruneTopTraderDataBefore(cutoff: number): Promise<void>;
  getLastOHLCVTimestamps(
    interval: OHLCVTimeframe
  ): Promise<Record<string, number | undefined>>;
  getLastAggTradeCheckpoint(
    symbol: string,
    marketType: AggTrade['marketType']
  ): Promise<{ tradeId: number; tradeTime: number } | undefined>;
  getLastTopTraderTimestamp(): Promise<number | undefined>;
}

export interface ISymbolManager {
  updateSymbols(): Promise<void>;
  scheduleDailyUpdate(): void;
  getActiveSymbolsByMarket(market: MarketType): Promise<SymbolMetadata[]>;
  getAllActiveSymbols(): Promise<SymbolMetadata[]>;
}

export interface IAggTradeDatabaseManager {
  initialize(): Promise<void>;
  saveAggTrades(asset: string, trades: AggTrade[]): Promise<void>;
  getLastAggTradeCheckpoint(
    asset: string,
    symbol: string,
    marketType: AggTrade['marketType']
  ): Promise<{ tradeId: number; tradeTime: number } | undefined>;
  close(): Promise<void>;
}

export interface RateLimiterRequest {
  weight: number;
  identifier?: string;
  priority?: number;
}

export interface IRateLimiter {
  schedule<T>(request: RateLimiterRequest, task: () => Promise<T>): Promise<T>;
  registerEndpoint(key: string, capacity: number, intervalMs: number): void;
  getUsageSnapshot(): RateLimiterUsageSnapshot;
}

export interface RateLimiterUsageSnapshot {
  endpoints: Array<{
    key: string;
    availableTokens: number;
    capacity: number;
    refillIntervalMs: number;
    queueLength: number;
  }>;
}
