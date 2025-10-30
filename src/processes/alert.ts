import '../utils/setup-logging';
import { initializeConfig, getConfig } from '../utils/config';
import { logger } from '../utils/logger';
import { DatabaseManager, AlertService } from '../services';
import { AlertQueueProcessor } from '../services/alert-queue-processor';
import { CvdAggregatorConfig } from '../types';

async function bootstrap(): Promise<void> {
  await initializeConfig();
  const config = getConfig();

  logger.setLevel(config.logLevel);

  if (!config.enableCvdAlerts) {
    logger.warn('CVD alerts are disabled; exiting alert dispatcher.');
    process.exit(0);
    return;
  }

  logger.info('Starting Binance alert dispatcher');

  const databaseManager = new DatabaseManager(config.databasePath);
  await databaseManager.initialize();
  await databaseManager.runMigrations();

  const alertService = new AlertService(databaseManager, {
    webhookUrl: config.discordWebhookUrl,
    displayNameMap: buildDisplayNameMap(config.cvdAggregators),
  });

  const processor = new AlertQueueProcessor(databaseManager, alertService, {
    pollIntervalMs: config.alertQueuePollIntervalMs,
    batchSize: config.alertQueueBatchSize,
    maxAttempts: config.alertQueueMaxAttempts,
  });

  bindProcessorEvents(processor);
  setupProcessHandlers(processor);

  await processor.start();
  logger.info('Binance alert dispatcher is running');
}

function buildDisplayNameMap(aggregators: CvdAggregatorConfig[]): Record<string, string> {
  const map: Record<string, string> = {};
  for (const aggregator of aggregators) {
    map[aggregator.id] = aggregator.displayName;
  }
  return map;
}

function bindProcessorEvents(processor: AlertQueueProcessor): void {
  processor.on('alertSent', (record) => {
    logger.info('Dispatched Binance CVD alert', {
      alertId: record.id,
      symbol: record.payload.symbol,
      triggerSource: record.payload.triggerSource,
      triggerZ: record.payload.triggerZScore,
    });
  });

  processor.on('alertFailed', ({ record, error }) => {
    logger.error('Failed to dispatch Binance CVD alert', {
      alertId: record.id,
      symbol: record.payload.symbol,
      error: error.message,
    });
  });

  processor.on('error', (error) => {
    logger.error('Alert queue processor error', error);
  });
}

function setupProcessHandlers(processor: AlertQueueProcessor): void {
  const shutdown = async (signal: NodeJS.Signals) => {
    logger.info(`Received ${signal}, shutting down alert dispatcher...`);
    await processor.stop().catch((error) => {
      logger.error('Failed to stop alert processor gracefully', error);
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
  logger.error('Fatal error during alert dispatcher bootstrap', error);
  process.exit(1);
});
