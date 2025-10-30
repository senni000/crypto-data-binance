import path from 'path';
import '../utils/setup-logging';
import { initializeConfig, getConfig } from '../utils/config';
import { logger } from '../utils/logger';
import {
  DatabaseManager,
  SymbolManager,
  RateLimiter,
  BinanceRestClient,
  AggTradeDatabaseManager,
  DataCollector,
  AggTradeCollector,
  DatabaseBackupScheduler,
  TradeDataCollector,
} from '../services';
import { CvdAggregatorConfig, CvdStreamConfig, MarketType } from '../types';

async function bootstrap(): Promise<void> {
  await initializeConfig();
  const config = getConfig();

  logger.setLevel(config.logLevel);
  logger.info('Starting Binance ingestion process');

  const rateLimiter = new RateLimiter();
  registerRateLimiters(rateLimiter, config.rateLimitBuffer);

  const databaseManager = new DatabaseManager(config.databasePath);
  await databaseManager.initialize();
  await databaseManager.runMigrations();

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

  const tradeDataCollector = new TradeDataCollector(databaseManager, {
    spotWsUrl: config.binanceSpotWsUrl,
    usdMWsUrl: config.binanceUsdMWsUrl,
    coinMWsUrl: config.binanceCoinMWsUrl,
    subscriptions: buildTradeSubscriptions(config.cvdAggregators),
    flushIntervalMs: config.tradeFlushIntervalMs,
    maxBufferSize: config.tradeMaxBufferSize,
  });

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

  const backupScheduler = config.databaseBackupEnabled
    ? new DatabaseBackupScheduler({
        sourcePath: config.databasePath,
        targetDirectory: config.databaseBackupDirectory,
        intervalMs: config.databaseBackupInterval,
        databaseManager,
        ...(config.databaseBackupSingleFile ? { singleFileName: 'binance_backup.sqlite' } : {}),
      })
    : null;

  bindTradeCollectorEvents(tradeDataCollector);
  bindDataCollectorEvents(dataCollector);

  setupProcessHandlers({
    dataCollector,
    tradeDataCollector,
    aggTradeCollector,
    backupScheduler,
  });

  await tradeDataCollector.start();
  await dataCollector.start();
  await aggTradeCollector.start();
  if (backupScheduler) {
    backupScheduler.start();
  }

  logger.info('Binance ingestion process is running');
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

function buildTradeSubscriptions(
  aggregators: CvdAggregatorConfig[]
): Array<{ symbol: string; marketType: MarketType; streamType: NonNullable<CvdStreamConfig['streamType']> }> {
  const deduped = new Map<string, { symbol: string; marketType: MarketType; streamType: NonNullable<CvdStreamConfig['streamType']> }>();

  for (const aggregator of aggregators) {
    for (const stream of aggregator.streams) {
      const streamType = stream.streamType ?? 'aggTrade';
      const symbol = stream.symbol.toUpperCase();
      const key = `${stream.marketType}:${symbol}:${streamType}`;
      if (!deduped.has(key)) {
        deduped.set(key, {
          symbol,
          marketType: stream.marketType,
          streamType,
        });
      }
    }
  }

  return Array.from(deduped.values());
}

function bindTradeCollectorEvents(tradeCollector: TradeDataCollector): void {
  tradeCollector.on('tradeDataSaved', (count: number) => {
    logger.debug(`Persisted ${count} Binance trades`);
  });

  tradeCollector.on('websocketError', (error) => {
    logger.error('Binance trade WebSocket error', error);
  });

  tradeCollector.on('error', (error) => {
    logger.error('Binance trade collector error', error);
  });
}

function bindDataCollectorEvents(dataCollector: DataCollector): void {
  dataCollector.on('restError', (error) => {
    logger.error('Binance REST data collector error', error);
  });

  dataCollector.on('topTraderStored', (payload) => {
    logger.info('Stored Binance top trader snapshots', payload);
  });
}

function setupProcessHandlers(params: {
  dataCollector: DataCollector;
  tradeDataCollector: TradeDataCollector;
  aggTradeCollector: AggTradeCollector;
  backupScheduler: DatabaseBackupScheduler | null;
}): void {
  const { dataCollector, tradeDataCollector, aggTradeCollector, backupScheduler } = params;

  const shutdown = async (signal: NodeJS.Signals) => {
    logger.info(`Received ${signal}, shutting down ingestion process...`);
    if (backupScheduler) {
      backupScheduler.stop();
    }
    await aggTradeCollector.stop();
    await tradeDataCollector.stopCollection();
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
    process.exit(1);
  });
}

void bootstrap().catch((error) => {
  logger.error('Fatal error during ingestion bootstrap', error);
  process.exit(1);
});
