import './utils/setup-logging';
import { initializeConfig, getConfig } from './utils/config';
import { logger } from './utils/logger';
import {
  DatabaseManager,
  SymbolManager,
  BinanceRestClient,
  BinanceWebSocketManager,
  RateLimiter,
  DataCollector,
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

  const wsManager = new BinanceWebSocketManager({
    spotWsUrl: config.binanceWebSocketBaseUrl,
    usdMWsUrl: config.binanceUsdMWebSocketBaseUrl,
    coinMWsUrl: config.binanceCoinMWebSocketBaseUrl,
    reconnectIntervalMs: config.wsReconnectInterval,
    maxSymbolsPerStream: config.wsMaxSymbolsPerStream,
  });

  const dataCollector = new DataCollector(
    databaseManager,
    symbolManager,
    restClient,
    wsManager,
    {
      restIntervals: {
        '30m': 30 * 60 * 1000,
        '1d': 24 * 60 * 60 * 1000,
      },
      topTraderIntervalMs: 5 * 60 * 1000,
      bufferFlushIntervalMs: 5_000,
      maxBufferSize: 1_000,
    }
  );

  const backupScheduler = config.databaseBackupEnabled
    ? new DatabaseBackupScheduler({
        sourcePath: config.databasePath,
        targetDirectory: config.databaseBackupDirectory,
        intervalMs: config.databaseBackupInterval,
        databaseManager,
      })
    : null;

  setupProcessHandlers(dataCollector, backupScheduler);

  await dataCollector.start();
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
  rateLimiter.registerEndpoint('topTrader:positions', applyBuffer(1_200), minute);
  rateLimiter.registerEndpoint('topTrader:accounts', applyBuffer(1_200), minute);
}

function setupProcessHandlers(
  dataCollector: DataCollector,
  backupScheduler: DatabaseBackupScheduler | null
): void {
  const shutdown = async (signal: NodeJS.Signals) => {
    logger.info(`Received ${signal}, shutting down gracefully...`);
    if (backupScheduler) {
      backupScheduler.stop();
    }
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
