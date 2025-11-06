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
  TradeDataCollector,
  LiquidationDataCollector,
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

  const liquidationCollector = new LiquidationDataCollector(databaseManager, {
    usdMWsUrl: config.binanceUsdMWsUrl,
    coinMWsUrl: config.binanceCoinMWsUrl,
    subscriptions: buildLiquidationSubscriptions(config.cvdAggregators),
    flushIntervalMs: config.liquidationFlushIntervalMs,
    maxBufferSize: config.liquidationMaxBufferSize,
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

  bindTradeCollectorEvents(tradeDataCollector);
  bindDataCollectorEvents(dataCollector);
  bindLiquidationCollectorEvents(liquidationCollector);

  setupProcessHandlers({
    dataCollector,
    tradeDataCollector,
    aggTradeCollector,
    liquidationCollector,
  });

  await tradeDataCollector.start();
  await liquidationCollector.start();
  await dataCollector.start();
  await aggTradeCollector.start();

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

function buildLiquidationSubscriptions(
  aggregators: CvdAggregatorConfig[]
): Array<{ symbol: string; marketType: Extract<MarketType, 'USDT-M' | 'COIN-M'> }> {
  const deduped = new Map<string, { symbol: string; marketType: Extract<MarketType, 'USDT-M' | 'COIN-M'> }>();

  for (const aggregator of aggregators) {
    for (const stream of aggregator.streams) {
      if (stream.marketType !== 'USDT-M' && stream.marketType !== 'COIN-M') {
        continue;
      }

      const symbol = stream.symbol.toUpperCase();
      // 現状はBTC系の先物清算のみ追跡する
      if (!symbol.includes('BTC')) {
        continue;
      }

      const market = stream.marketType;
      const key = `${market}:${symbol}`;

      if (!deduped.has(key)) {
        deduped.set(key, { symbol, marketType: market });
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

function bindLiquidationCollectorEvents(liquidationCollector: LiquidationDataCollector): void {
  liquidationCollector.on('liquidationDataSaved', (count: number) => {
    logger.debug(`Persisted ${count} Binance liquidations`);
  });

  liquidationCollector.on('websocketError', (error) => {
    logger.error('Binance liquidation WebSocket error', error);
  });

  liquidationCollector.on('error', (error) => {
    logger.error('Binance liquidation collector error', error);
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
  liquidationCollector: LiquidationDataCollector;
}): void {
  const { dataCollector, tradeDataCollector, aggTradeCollector, liquidationCollector } = params;

  const shutdown = async (signal: NodeJS.Signals) => {
    logger.info(`Received ${signal}, shutting down ingestion process...`);
    await aggTradeCollector.stop();
    await liquidationCollector.stop();
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
