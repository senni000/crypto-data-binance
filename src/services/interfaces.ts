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
  TradeData,
  CVDData,
  AlertHistory,
  CvdAggregatorConfig,
  TradeDataRow,
  AlertQueueRecord,
} from '../types';
import { LogLevel } from '../types/config';
import { CvdAlertPayload } from '@crypto-data/cvd-core';

export interface AppConfig {
  discordWebhookUrl: string;
  enableCvdAlerts: boolean;
  databasePath: string;
  databaseBackupEnabled: boolean;
  databaseBackupDirectory: string;
  databaseBackupInterval: number;
  databaseBackupSingleFile: boolean;
  databaseRetentionMs: number | null;
  logLevel: LogLevel;
  binanceRestBaseUrl: string;
  binanceUsdMRestBaseUrl: string;
  binanceCoinMRestBaseUrl: string;
  binanceSpotWsUrl: string;
  binanceUsdMWsUrl: string;
  binanceCoinMWsUrl: string;
  rateLimitBuffer: number;
  restRequestTimeout: number;
  symbolUpdateHourUtc: number;
  aggTradeDataDirectory: string;
  cvdZScoreThreshold: number;
  cvdAggregators: CvdAggregatorConfig[];
  tradeFlushIntervalMs: number;
  tradeMaxBufferSize: number;
  cvdAggregationBatchSize: number;
  cvdAggregationPollIntervalMs: number;
  cvdAlertSuppressionMinutes: number;
  alertQueuePollIntervalMs: number;
  alertQueueBatchSize: number;
  alertQueueMaxAttempts: number;
}

export interface IDataCollector {
  start(): Promise<void>;
  stop(): Promise<void>;
}

export interface ITradeDataCollector {
  start(): Promise<void>;
  stopCollection(): Promise<void>;
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
  saveTradeData(data: TradeData[]): Promise<void>;
  saveCVDData(data: CVDData): Promise<void>;
  getCVDDataSince(symbol: string, since: number): Promise<CVDData[]>;
  saveAlertHistory(alert: AlertHistory): Promise<void>;
  getRecentAlerts(alertType: string, symbol: string, minutes?: number): Promise<AlertHistory[]>;
  getTradeDataSinceRowId(
    filters: Array<{ symbol: string; marketType: MarketType; streamType: TradeData['streamType'] }>,
    lastRowId: number,
    limit: number
  ): Promise<TradeDataRow[]>;
  getProcessingState(processName: string, key: string): Promise<ProcessingState | null>;
  saveProcessingState(processName: string, key: string, state: ProcessingState): Promise<void>;
  enqueueAlert(alertType: string, payload: CvdAlertPayload): Promise<number>;
  getPendingAlerts(limit: number): Promise<AlertQueueRecord[]>;
  markAlertAttempt(id: number): Promise<void>;
  markAlertProcessed(id: number, options?: { clearError?: boolean }): Promise<void>;
  markAlertFailure(id: number, error: string): Promise<void>;
  hasRecentAlertOrPending(alertType: string, symbol: string, sinceTimestamp: number): Promise<boolean>;
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

export interface IAlertService {
  sendCvdAlert(payload: {
    timestamp: number;
    zScore: number;
    deltaZScore: number;
    threshold: number;
    cumulativeValue: number;
    delta: number;
    symbol: string;
    triggerSource: 'cumulative' | 'delta';
    triggerZScore: number;
  }): Promise<void>;
}

export interface ProcessingState {
  lastRowId: number;
  lastTimestamp: number;
}
