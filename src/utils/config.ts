/**
 * 環境変数の読み込みと検証
 */

import * as dotenv from 'dotenv';
import * as os from 'os';
import * as path from 'path';
import { AppConfig } from '../services/interfaces';
import { LogLevel } from '../types/config';

dotenv.config();

const REQUIRED_WS_PROTOCOLS = new Set(['ws:', 'wss:']);

export class ConfigManager {
  private config: AppConfig | null = null;

  async loadConfig(): Promise<void> {
    const databasePath = this.expandPath(
      this.getEnvVar('DATABASE_PATH', '~/workspace/crypto-data/data/binance.db')
    );
    const databaseBackupDirectory = this.expandPath(
      this.getEnvVar('DATABASE_BACKUP_PATH', '/Volumes/buffalohd/crypto-data/backups/binance')
    );

    const config: AppConfig = {
      databasePath,
      databaseBackupEnabled: this.getBooleanEnvVar('DATABASE_BACKUP_ENABLED', true),
      databaseBackupDirectory,
      databaseBackupInterval: this.getNumberEnvVar('DATABASE_BACKUP_INTERVAL_MS', 24 * 60 * 60 * 1000),
      databaseBackupSingleFile: this.getBooleanEnvVar('DATABASE_BACKUP_SINGLE_FILE', false),
      logLevel: this.getLogLevel(this.getEnvVar('LOG_LEVEL', 'info') as LogLevel),
      binanceRestBaseUrl: this.getEnvVar('BINANCE_REST_URL', 'https://api.binance.com'),
      binanceUsdMRestBaseUrl: this.getEnvVar('BINANCE_USDM_REST_URL', 'https://fapi.binance.com'),
      binanceCoinMRestBaseUrl: this.getEnvVar('BINANCE_COINM_REST_URL', 'https://dapi.binance.com'),
      binanceWebSocketBaseUrl: this.getWebSocketUrl(
        this.getEnvVar('BINANCE_WS_URL', 'wss://stream.binance.com:9443/ws')
      ),
      binanceUsdMWebSocketBaseUrl: this.getWebSocketUrl(
        this.getEnvVar('BINANCE_USDM_WS_URL', 'wss://fstream.binance.com/ws')
      ),
      binanceCoinMWebSocketBaseUrl: this.getWebSocketUrl(
        this.getEnvVar('BINANCE_COINM_WS_URL', 'wss://dstream.binance.com/ws')
      ),
      rateLimitBuffer: this.getNumberEnvVar('RATE_LIMIT_BUFFER', 0.1),
      restRequestTimeout: this.getNumberEnvVar('REST_REQUEST_TIMEOUT_MS', 10_000),
      wsReconnectInterval: this.getNumberEnvVar('WS_RECONNECT_INTERVAL_MS', 5_000),
      wsMaxSymbolsPerStream: this.getNumberEnvVar('WS_MAX_SYMBOLS_PER_STREAM', 300),
      symbolUpdateHourUtc: this.getNumberEnvVar('SYMBOL_UPDATE_HOUR_UTC', 1),
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

    if (config.rateLimitBuffer < 0 || config.rateLimitBuffer >= 1) {
      errors.push('RATE_LIMIT_BUFFER must be between 0 (inclusive) and 1 (exclusive)');
    }

    if (config.wsMaxSymbolsPerStream <= 0) {
      errors.push('WS_MAX_SYMBOLS_PER_STREAM must be positive');
    }

    if (config.symbolUpdateHourUtc < 0 || config.symbolUpdateHourUtc > 23) {
      errors.push('SYMBOL_UPDATE_HOUR_UTC must be between 0 and 23');
    }

    if (errors.length > 0) {
      errors.forEach((err) => console.error(err));
      throw new Error('Configuration validation failed');
    }
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

  private getWebSocketUrl(value: string): string {
    try {
      const url = new URL(value);
      if (!REQUIRED_WS_PROTOCOLS.has(url.protocol)) {
        throw new Error(`Expected ws:// or wss:// but got ${url.protocol}`);
      }
      return value;
    } catch (error) {
      throw new Error(`Invalid BINANCE_WS_URL: ${(error as Error).message}`);
    }
  }

  private expandPath(filePath: string): string {
    if (filePath.startsWith('~/')) {
      return path.join(os.homedir(), filePath.slice(2));
    }
    return filePath;
  }
}

export const configManager = new ConfigManager();

export async function initializeConfig(): Promise<void> {
  await configManager.loadConfig();
}

export function getConfig(): AppConfig {
  return configManager.getConfig();
}
