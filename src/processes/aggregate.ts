import '../utils/setup-logging';
import { initializeConfig, getConfig } from '../utils/config';
import { logger } from '../utils/logger';
import { DatabaseManager } from '../services';
import { CvdAggregationWorker } from '../services/cvd-aggregation-worker';

async function bootstrap(): Promise<void> {
  await initializeConfig();
  const config = getConfig();

  logger.setLevel(config.logLevel);
  logger.info('Starting Binance CVD aggregation worker');

  const databaseManager = new DatabaseManager(config.databasePath);
  await databaseManager.initialize();
  await databaseManager.runMigrations();

  const worker = new CvdAggregationWorker(
    databaseManager,
    config.cvdAggregators,
    config.cvdZScoreThreshold,
    config.enableCvdAlerts,
    {
      batchSize: config.cvdAggregationBatchSize,
      pollIntervalMs: config.cvdAggregationPollIntervalMs,
      suppressionWindowMinutes: config.cvdAlertSuppressionMinutes,
    }
  );

  bindWorkerEvents(worker);
  setupProcessHandlers(worker);

  await worker.start();
  logger.info('Binance CVD aggregation worker is running');
}

function bindWorkerEvents(worker: CvdAggregationWorker): void {
  worker.on('batchProcessed', ({ aggregatorId, count }) => {
    logger.debug(`Processed ${count} trades for CVD aggregator ${aggregatorId}`);
  });

  worker.on('error', (error) => {
    logger.error('CVD aggregation worker error', error);
  });
}

function setupProcessHandlers(worker: CvdAggregationWorker): void {
  const shutdown = async (signal: NodeJS.Signals) => {
    logger.info(`Received ${signal}, shutting down CVD aggregation worker...`);
    await worker.stop().catch((error) => {
      logger.error('Failed to stop CVD aggregation worker gracefully', error);
    });
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
  logger.error('Fatal error during CVD aggregation bootstrap', error);
  process.exit(1);
});
