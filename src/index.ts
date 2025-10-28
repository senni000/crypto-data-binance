import path from 'path';
import './utils/setup-logging';
import { initializeConfig, getConfig } from './utils/config';
import { logger } from './utils/logger';
import {
  DatabaseManager,
  SymbolManager,
  BinanceRestClient,
  AggTradeDatabaseManager,
  RateLimiter,
  DataCollector,
  AggTradeCollector,
  DatabaseBackupScheduler,
} from './services';

async function bootstrap(): Promise<void> {
  await initializeConfig();
  const config = getConfig();

  logger.setLevel(config.logLevel);
  logger.info('Starting Binance data collector');

  const rateLimiter = new RateLimiter();
  registerRateLimiters(rateLimiter, config.rateLimitBuffer);

  const databaseManager = new DatabaseManager(config.databasePath);
  const symbolManager = new SymbolManager(databaseManager, rateLimiter, {
    spotBaseUrl: config.binanceRestBaseUrl,
    usdMBaseUrl: config.binanceUsdMRestBaseUrl,
    coinMBaseUrl: config.binanceCoinMRestBaseUrl,
    symbolUpdateHourUtc: config.symbolUpdateHourUtc,
  });

  const restClient = new BinanceRestClient(rateLimiter, {
    spotBaseUrl: config.binanceRestBaseUrl,
    usdMBaseUrl: config.binanceUsdMRestBaseUrl,
    coinMBaseUrl: config.binanceCoinMRestBaseUrl,
    timeout: config.restRequestTimeout,
  });

  const aggTradeDatabaseManager = new AggTradeDatabaseManager(config.aggTradeDataDirectory);

  const dataCollector = new DataCollector(
    databaseManager,
    symbolManager,
    restClient,
    {
      topTraderIntervalMs: 5 * 60 * 1000,
      topTraderRequestDelayMs: 3_000,
      topTraderMaxRetries: 3,
      topTraderRetryDelayMs: 5_000,
    }
  );

  const aggTradeCollector = new AggTradeCollector(
    aggTradeDatabaseManager,
    symbolManager,
    restClient,
    {
      assetListPath: path.resolve(process.cwd(), 'coinmarketcap_top100.csv'),
      fetchIntervalMs: 60 * 60 * 1000,
      restLimit: 1_000,
      initialLookbackMs: 12 * 60 * 60 * 1000,
      maxRetries: 3,
      retryDelayMs: 5_000,
    }
  );

  const backupOptions = {
    sourcePath: config.databasePath,
    targetDirectory: config.databaseBackupDirectory,
    intervalMs: config.databaseBackupInterval,
    databaseManager,
    ...(config.databaseBackupSingleFile ? { singleFileName: 'binance_backup.sqlite' } : {}),
  };

  const backupScheduler = config.databaseBackupEnabled
    ? new DatabaseBackupScheduler(backupOptions)
    : null;

  setupProcessHandlers(dataCollector, aggTradeCollector, backupScheduler);

  await dataCollector.start();
  await aggTradeCollector.start();
  if (backupScheduler) {
    backupScheduler.start();
  }

  logger.info('Binance data collector is running');
}

function registerRateLimiters(rateLimiter: RateLimiter, buffer: number): void {
  const minute = 60_000;
  const applyBuffer = (base: number) => Math.max(1, Math.floor(base * (1 - buffer)));

  rateLimiter.registerEndpoint('exchangeInfo:spot', applyBuffer(1_200), minute);
  rateLimiter.registerEndpoint('exchangeInfo:usdm', applyBuffer(1_200), minute);
  rateLimiter.registerEndpoint('exchangeInfo:coinm', applyBuffer(1_200), minute);
  rateLimiter.registerEndpoint('klines:SPOT', applyBuffer(1_200), minute);
  rateLimiter.registerEndpoint('klines:USDT-M', applyBuffer(1_200), minute);
  rateLimiter.registerEndpoint('klines:COIN-M', applyBuffer(1_200), minute);
  rateLimiter.registerEndpoint('aggTrades:SPOT', applyBuffer(1_200), minute);
  rateLimiter.registerEndpoint('aggTrades:USDT-M', applyBuffer(1_200), minute);
  rateLimiter.registerEndpoint('topTrader:positions', applyBuffer(1_200), minute);
  rateLimiter.registerEndpoint('topTrader:accounts', applyBuffer(1_200), minute);
}

function setupProcessHandlers(
  dataCollector: DataCollector,
  aggTradeCollector: AggTradeCollector,
  backupScheduler: DatabaseBackupScheduler | null
): void {
  const shutdown = async (signal: NodeJS.Signals) => {
    logger.info(`Received ${signal}, shutting down gracefully...`);
    if (backupScheduler) {
      backupScheduler.stop();
    }
    await aggTradeCollector.stop();
    await dataCollector.stop();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  process.on('unhandledRejection', (reason) => {
    logger.error('Unhandled promise rejection', reason);
  });

  process.on('uncaughtException', (error) => {
    logger.error('Uncaught exception', error);
  });
}

void bootstrap().catch((error) => {
  logger.error('Fatal error during bootstrap', error);
  process.exit(1);
});
