/**
 * 環境変数の読み込みと検証
 */

import * as dotenv from 'dotenv';
import * as os from 'os';
import * as path from 'path';
import { AppConfig } from '../services/interfaces';
import { LogLevel } from '../types/config';
import { CvdAggregatorConfig, CvdStreamConfig, MarketType } from '../types';

dotenv.config();

export class ConfigManager {
  private config: AppConfig | null = null;

  async loadConfig(): Promise<void> {
    const databasePath = this.expandPath(
      this.getEnvVar('DATABASE_PATH', '~/workspace/crypto-data/data/binance.db')
    );
    const databaseBackupDirectory = this.expandPath(
      this.getEnvVar('DATABASE_BACKUP_PATH', '/Volumes/buffalohd/crypto-data/backups/binance')
    );

    const aggTradeDataDirectory = this.expandPath(
      this.getEnvVar(
        'AGG_TRADE_DATA_DIR',
        path.join(path.dirname(databasePath), 'agg-trades')
      )
    );

    const enableCvdAlerts = this.getBooleanEnvVar('ENABLE_CVD_ALERTS', false);
    const discordWebhookUrl = enableCvdAlerts
      ? this.getRequiredEnvVar('DISCORD_WEBHOOK_URL')
      : this.getEnvVar('DISCORD_WEBHOOK_URL', '');

    const config: AppConfig = {
      discordWebhookUrl,
      enableCvdAlerts,
      databasePath,
      databaseBackupEnabled: this.getBooleanEnvVar('DATABASE_BACKUP_ENABLED', true),
      databaseBackupDirectory,
      databaseBackupInterval: this.getNumberEnvVar('DATABASE_BACKUP_INTERVAL_MS', 24 * 60 * 60 * 1000),
      databaseBackupSingleFile: this.getBooleanEnvVar('DATABASE_BACKUP_SINGLE_FILE', false),
      logLevel: this.getLogLevel(this.getEnvVar('LOG_LEVEL', 'info') as LogLevel),
      binanceRestBaseUrl: this.getEnvVar('BINANCE_REST_URL', 'https://api.binance.com'),
      binanceUsdMRestBaseUrl: this.getEnvVar('BINANCE_USDM_REST_URL', 'https://fapi.binance.com'),
      binanceCoinMRestBaseUrl: this.getEnvVar('BINANCE_COINM_REST_URL', 'https://dapi.binance.com'),
      binanceSpotWsUrl: this.getEnvVar('BINANCE_SPOT_WS_URL', 'wss://stream.binance.com:9443/stream'),
      binanceUsdMWsUrl: this.getEnvVar('BINANCE_USDM_WS_URL', 'wss://fstream.binance.com/stream'),
      binanceCoinMWsUrl: this.getEnvVar('BINANCE_COINM_WS_URL', 'wss://dstream.binance.com/stream'),
      rateLimitBuffer: this.getNumberEnvVar('RATE_LIMIT_BUFFER', 0.1),
      restRequestTimeout: this.getNumberEnvVar('REST_REQUEST_TIMEOUT_MS', 10_000),
      symbolUpdateHourUtc: this.getNumberEnvVar('SYMBOL_UPDATE_HOUR_UTC', 1),
      aggTradeDataDirectory,
      cvdZScoreThreshold: this.getNumberEnvVar('CVD_ZSCORE_THRESHOLD', 2.0),
      cvdAggregators: this.resolveCvdAggregators(),
      tradeFlushIntervalMs: this.getNumberEnvVar('BINANCE_TRADE_FLUSH_INTERVAL_MS', 5_000),
      tradeMaxBufferSize: this.getNumberEnvVar('BINANCE_TRADE_MAX_BUFFER_SIZE', 1_000),
      cvdAggregationBatchSize: this.getNumberEnvVar('CVD_AGGREGATION_BATCH_SIZE', 500),
      cvdAggregationPollIntervalMs: this.getNumberEnvVar('CVD_AGGREGATION_POLL_INTERVAL_MS', 2_000),
      cvdAlertSuppressionMinutes: this.getNumberEnvVar('CVD_ALERT_SUPPRESSION_MINUTES', 30),
      alertQueuePollIntervalMs: this.getNumberEnvVar('ALERT_QUEUE_POLL_INTERVAL_MS', 2_000),
      alertQueueBatchSize: this.getNumberEnvVar('ALERT_QUEUE_BATCH_SIZE', 20),
      alertQueueMaxAttempts: this.getNumberEnvVar('ALERT_QUEUE_MAX_ATTEMPTS', 5),
    };

    this.validateConfig(config);
    this.config = config;
  }

  getConfig(): AppConfig {
    if (!this.config) {
      throw new Error('Config has not been loaded. Call initializeConfig() first.');
    }
    return this.config;
  }

  private validateConfig(config: AppConfig): void {
    const errors: string[] = [];

    if (!config.databasePath) {
      errors.push('DATABASE_PATH is required');
    }

    if (config.databaseBackupEnabled && !config.databaseBackupDirectory) {
      errors.push('DATABASE_BACKUP_PATH is required when DATABASE_BACKUP_ENABLED=true');
    }

    if (!config.aggTradeDataDirectory) {
      errors.push('AGG_TRADE_DATA_DIR must not be empty');
    }

    if (config.enableCvdAlerts !== false) {
      if (!config.discordWebhookUrl) {
        errors.push('DISCORD_WEBHOOK_URL is required when ENABLE_CVD_ALERTS is true');
      } else if (!this.isValidDiscordWebhookUrl(config.discordWebhookUrl)) {
        errors.push('DISCORD_WEBHOOK_URL must be a valid Discord webhook URL');
      }
    } else if (config.discordWebhookUrl && !this.isValidDiscordWebhookUrl(config.discordWebhookUrl)) {
      errors.push('DISCORD_WEBHOOK_URL must be a valid Discord webhook URL when provided');
    }

    const wsTargets: Array<{ name: string; value: string }> = [
      { name: 'BINANCE_SPOT_WS_URL', value: config.binanceSpotWsUrl },
      { name: 'BINANCE_USDM_WS_URL', value: config.binanceUsdMWsUrl },
      { name: 'BINANCE_COINM_WS_URL', value: config.binanceCoinMWsUrl },
    ];

    for (const target of wsTargets) {
      if (!target.value) {
        errors.push(`${target.name} is required`);
      } else if (!this.isValidWebSocketUrl(target.value)) {
        errors.push(`${target.name} must be a valid WebSocket URL`);
      }
    }

    if (config.rateLimitBuffer < 0 || config.rateLimitBuffer >= 1) {
      errors.push('RATE_LIMIT_BUFFER must be between 0 (inclusive) and 1 (exclusive)');
    }

    if (config.symbolUpdateHourUtc < 0 || config.symbolUpdateHourUtc > 23) {
      errors.push('SYMBOL_UPDATE_HOUR_UTC must be between 0 and 23');
    }

    if (config.cvdZScoreThreshold <= 0) {
      errors.push('CVD_ZSCORE_THRESHOLD must be greater than 0');
    }

    if (!config.cvdAggregators || config.cvdAggregators.length === 0) {
      errors.push('At least one CVD aggregator must be configured');
    } else {
      const ids = new Set<string>();
      for (const aggregator of config.cvdAggregators) {
        if (!aggregator.id || aggregator.id.trim() === '') {
          errors.push('Each CVD aggregator must have a non-empty id');
        } else if (ids.has(aggregator.id)) {
          errors.push(`Duplicate CVD aggregator id detected: ${aggregator.id}`);
        } else {
          ids.add(aggregator.id);
        }

        if (!aggregator.displayName || aggregator.displayName.trim() === '') {
          errors.push(`CVD aggregator ${aggregator.id} must have a displayName`);
        }

        if (
          aggregator.alertsEnabled !== undefined &&
          typeof aggregator.alertsEnabled !== 'boolean'
        ) {
          errors.push(`CVD aggregator ${aggregator.id} has invalid alertsEnabled flag`);
        }

        if (!aggregator.streams || aggregator.streams.length === 0) {
          errors.push(`CVD aggregator ${aggregator.id} must define at least one stream`);
        } else {
          for (const stream of aggregator.streams) {
            if (!stream.symbol || stream.symbol.trim() === '') {
              errors.push(`CVD aggregator ${aggregator.id} has a stream with empty symbol`);
            }
            if (!this.isValidMarketType(stream.marketType)) {
              errors.push(
                `CVD aggregator ${aggregator.id} has unsupported marketType ${stream.marketType}`
              );
            }
            if (stream.streamType && !this.isValidStreamType(stream.streamType)) {
              errors.push(
                `CVD aggregator ${aggregator.id} stream ${stream.symbol} has unsupported streamType ${stream.streamType}`
              );
            }
          }
        }
      }
    }

    if (config.tradeFlushIntervalMs <= 0) {
      errors.push('BINANCE_TRADE_FLUSH_INTERVAL_MS must be greater than 0');
    }

    if (config.tradeMaxBufferSize <= 0) {
      errors.push('BINANCE_TRADE_MAX_BUFFER_SIZE must be greater than 0');
    }

    if (config.cvdAggregationBatchSize <= 0) {
      errors.push('CVD_AGGREGATION_BATCH_SIZE must be greater than 0');
    }

    if (config.cvdAggregationPollIntervalMs <= 0) {
      errors.push('CVD_AGGREGATION_POLL_INTERVAL_MS must be greater than 0');
    }

    if (config.cvdAlertSuppressionMinutes <= 0) {
      errors.push('CVD_ALERT_SUPPRESSION_MINUTES must be greater than 0');
    }

    if (config.alertQueuePollIntervalMs <= 0) {
      errors.push('ALERT_QUEUE_POLL_INTERVAL_MS must be greater than 0');
    }

    if (config.alertQueueBatchSize <= 0) {
      errors.push('ALERT_QUEUE_BATCH_SIZE must be greater than 0');
    }

    if (config.alertQueueMaxAttempts <= 0) {
      errors.push('ALERT_QUEUE_MAX_ATTEMPTS must be greater than 0');
    }

    if (errors.length > 0) {
      errors.forEach((err) => console.error(err));
      throw new Error('Configuration validation failed');
    }
  }

  private getRequiredEnvVar(name: string): string {
    const value = process.env[name];
    if (!value) {
      throw new Error(`Required environment variable ${name} is not set`);
    }
    return value;
  }

  private getEnvVar(name: string, defaultValue: string): string {
    const value = process.env[name];
    return value === undefined ? defaultValue : value;
  }

  private getBooleanEnvVar(name: string, defaultValue: boolean): boolean {
    const value = process.env[name];
    if (value === undefined) {
      return defaultValue;
    }

    switch (value.toLowerCase()) {
      case '1':
      case 'true':
      case 'yes':
      case 'on':
        return true;
      case '0':
      case 'false':
      case 'no':
      case 'off':
        return false;
      default:
        console.warn(`Invalid boolean value for ${name}: ${value}. Using default: ${defaultValue}`);
        return defaultValue;
    }
  }

  private getNumberEnvVar(name: string, defaultValue: number): number {
    const value = process.env[name];
    if (value === undefined || value === '') {
      return defaultValue;
    }

    const parsed = Number(value);
    if (Number.isNaN(parsed)) {
      console.warn(`Invalid numeric value for ${name}: ${value}. Using default: ${defaultValue}`);
      return defaultValue;
    }
    return parsed;
  }

  private getLogLevel(value: LogLevel): LogLevel {
    if (['error', 'warn', 'info', 'debug'].includes(value)) {
      return value;
    }
    console.warn(`Invalid LOG_LEVEL ${value}. Falling back to info.`);
    return 'info';
  }

  private expandPath(filePath: string): string {
    if (filePath.startsWith('~/')) {
      return path.join(os.homedir(), filePath.slice(2));
    }
    return filePath;
  }

  private isValidDiscordWebhookUrl(url: string): boolean {
    try {
      const parsed = new URL(url);
      return (
        (parsed.protocol === 'https:' || parsed.protocol === 'http:') &&
        (parsed.hostname === 'discord.com' || parsed.hostname === 'discordapp.com') &&
        parsed.pathname.includes('/api/webhooks/')
      );
    } catch {
      return false;
    }
  }

  private isValidWebSocketUrl(url: string): boolean {
    try {
      const parsed = new URL(url);
      return parsed.protocol === 'ws:' || parsed.protocol === 'wss:';
    } catch {
      return false;
    }
  }

  private isValidStreamType(value: string): value is 'aggTrade' | 'trade' {
    return value === 'aggTrade' || value === 'trade';
  }

  private isValidMarketType(value: string): value is MarketType {
    return value === 'SPOT' || value === 'USDT-M' || value === 'COIN-M';
  }

  private resolveCvdAggregators(): CvdAggregatorConfig[] {
    const raw = process.env['BINANCE_CVD_GROUPS'];
    const fallback = this.getDefaultCvdAggregators();

    if (!raw || raw.trim() === '') {
      return fallback;
    }

    try {
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) {
        console.warn('BINANCE_CVD_GROUPS must be a JSON array. Falling back to defaults.');
        return fallback;
      }

      const aggregators: CvdAggregatorConfig[] = [];
      for (const entry of parsed) {
        const aggregator = this.normalizeAggregator(entry);
        if (aggregator) {
          aggregators.push(aggregator);
        }
      }

      if (aggregators.length === 0) {
        console.warn('No valid entries detected in BINANCE_CVD_GROUPS. Falling back to defaults.');
        return fallback;
      }

      return aggregators;
    } catch (error) {
      console.warn('Failed to parse BINANCE_CVD_GROUPS. Falling back to defaults.', error);
      return fallback;
    }
  }

  private normalizeAggregator(entry: any): CvdAggregatorConfig | null {
    if (!entry || typeof entry !== 'object') {
      return null;
    }

    const id = typeof entry.id === 'string' ? entry.id.trim() : '';
    const displayNameRaw = typeof entry.displayName === 'string' ? entry.displayName.trim() : '';
    const streamsRaw = Array.isArray(entry.streams) ? entry.streams : [];

    const streams: CvdStreamConfig[] = [];
    for (const streamEntry of streamsRaw) {
      const stream = this.normalizeStream(streamEntry);
      if (stream) {
        streams.push(stream);
      }
    }

    if (!id || streams.length === 0) {
      return null;
    }

    const alertsEnabled = this.normalizeAlertsEnabled(entry.alertsEnabled);

    const baseConfig = {
      id,
      displayName: displayNameRaw || id,
      streams,
    };

    if (alertsEnabled === undefined) {
      return baseConfig;
    }

    return {
      ...baseConfig,
      alertsEnabled,
    };
  }

  private normalizeStream(entry: any): CvdStreamConfig | null {
    if (!entry || typeof entry !== 'object') {
      return null;
    }

    const symbolRaw = typeof entry.symbol === 'string' ? entry.symbol.trim() : '';
    const marketRaw = typeof entry.marketType === 'string' ? entry.marketType.trim() : '';
    const streamRaw = typeof entry.streamType === 'string' ? entry.streamType.trim() : 'aggTrade';

    const normalizedSymbol = symbolRaw.toUpperCase();
    const normalizedMarket = this.normalizeMarketType(marketRaw);
    const normalizedStream: 'aggTrade' | 'trade' = this.isValidStreamType(streamRaw)
      ? (streamRaw as 'aggTrade' | 'trade')
      : 'aggTrade';

    if (!normalizedSymbol || !normalizedMarket) {
      return null;
    }

    return {
      symbol: normalizedSymbol,
      marketType: normalizedMarket,
      streamType: normalizedStream,
    };
  }

  private normalizeMarketType(value: string): MarketType | null {
    const normalized = value.replace(/[\s_-]/g, '').toUpperCase();
    switch (normalized) {
      case 'SPOT':
        return 'SPOT';
      case 'USDTM':
      case 'USDM':
      case 'USDT':
      case 'USDTPERP':
      case 'USDMPERP':
      case 'USDTMARGINED':
        return 'USDT-M';
      case 'COINM':
      case 'COIN':
      case 'COINPERP':
      case 'COIND':
        return 'COIN-M';
      default:
        if (value === 'USDT-M' || value === 'COIN-M') {
          return value as MarketType;
        }
        return null;
    }
  }

  private normalizeAlertsEnabled(value: unknown): boolean | undefined {
    if (value === undefined) {
      return undefined;
    }

    if (typeof value === 'boolean') {
      return value;
    }

    if (typeof value === 'number') {
      return value !== 0;
    }

    if (typeof value === 'string') {
      const normalized = value.trim().toLowerCase();
      if (['true', '1', 'yes', 'on'].includes(normalized)) {
        return true;
      }
      if (['false', '0', 'no', 'off'].includes(normalized)) {
        return false;
      }
    }

    return undefined;
  }

  private getDefaultCvdAggregators(): CvdAggregatorConfig[] {
    const futuresStreams: CvdStreamConfig[] = [
      { symbol: 'BTCUSDT', marketType: 'USDT-M', streamType: 'aggTrade' },
      { symbol: 'BTCBUSD', marketType: 'USDT-M', streamType: 'aggTrade' },
      { symbol: 'BTCUSD_PERP', marketType: 'COIN-M', streamType: 'aggTrade' },
    ];

    return [
      {
        id: 'BINANCE_SPOT_BTCFDUSD',
        displayName: 'Binance Spot BTCFDUSD',
        streams: [{ symbol: 'BTCFDUSD', marketType: 'SPOT', streamType: 'aggTrade' }],
      },
      {
        id: 'BINANCE_SPOT_BTCUSDT',
        displayName: 'Binance Spot BTCUSDT',
        streams: [{ symbol: 'BTCUSDT', marketType: 'SPOT', streamType: 'aggTrade' }],
      },
      {
        id: 'BINANCE_FUTURES_COMBINED',
        displayName: 'Binance Futures (Combined)',
        streams: futuresStreams,
      },
      {
        id: 'BINANCE_CVD_TOTAL',
        displayName: 'Binance Total CVD',
        streams: [
          { symbol: 'BTCFDUSD', marketType: 'SPOT', streamType: 'aggTrade' },
          { symbol: 'BTCUSDT', marketType: 'SPOT', streamType: 'aggTrade' },
          ...futuresStreams,
        ],
        alertsEnabled: false,
      },
    ];
  }
}

export const configManager = new ConfigManager();

export async function initializeConfig(): Promise<void> {
  await configManager.loadConfig();
}

export function getConfig(): AppConfig {
  return configManager.getConfig();
}
