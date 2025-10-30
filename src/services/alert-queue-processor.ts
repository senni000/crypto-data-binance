import { EventEmitter } from 'events';
import { IDatabaseManager, IAlertService } from './interfaces';
import { AlertQueueRecord } from '../types';
import { logger } from '../utils/logger';

export interface AlertQueueProcessorOptions {
  pollIntervalMs?: number;
  batchSize?: number;
  maxAttempts?: number;
}

export declare interface AlertQueueProcessor {
  on(event: 'alertSent', listener: (payload: AlertQueueRecord) => void): this;
  on(event: 'alertFailed', listener: (payload: { record: AlertQueueRecord; error: Error }) => void): this;
  on(event: 'idle', listener: () => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
}

export class AlertQueueProcessor extends EventEmitter {
  private readonly pollIntervalMs: number;
  private readonly batchSize: number;
  private readonly maxAttempts: number;
  private running = false;
  private processing = false;
  private timer: NodeJS.Timeout | null = null;

  constructor(
    private readonly databaseManager: IDatabaseManager,
    private readonly alertService: IAlertService,
    options: AlertQueueProcessorOptions = {}
  ) {
    super();
    this.pollIntervalMs = Math.max(500, Math.floor(options.pollIntervalMs ?? 2_000));
    this.batchSize = Math.max(1, Math.floor(options.batchSize ?? 20));
    this.maxAttempts = Math.max(1, Math.floor(options.maxAttempts ?? 5));
  }

  async start(): Promise<void> {
    if (this.running) {
      return;
    }
    this.running = true;
    await this.processQueue();
    this.scheduleNext();
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    if (this.processing) {
      await this.waitForIdle();
    }
  }

  private scheduleNext(): void {
    if (!this.running) {
      return;
    }
    this.timer = setTimeout(() => {
      void this.processQueue()
        .catch((error) => {
          this.emit('error', error instanceof Error ? error : new Error(String(error)));
          logger.error('Alert queue processing failed', error);
        })
        .finally(() => {
          if (this.running) {
            this.scheduleNext();
          }
        });
    }, this.pollIntervalMs);
  }

  private async processQueue(): Promise<void> {
    if (this.processing) {
      return;
    }
    this.processing = true;

    try {
      const records = await this.databaseManager.getPendingAlerts(this.batchSize);
      const exhausted = records.filter((record) => record.attemptCount >= this.maxAttempts);
      for (const record of exhausted) {
        logger.warn('Skipping alert due to retry exhaustion', {
          alertId: record.id,
          symbol: record.payload.symbol,
          attempts: record.attemptCount,
        });
        await this.databaseManager.markAlertFailure(record.id, 'Retry limit reached');
        await this.databaseManager.markAlertProcessed(record.id, { clearError: false });
      }

      const pending = records.filter((record) => record.attemptCount < this.maxAttempts);
      for (const record of pending) {
        if (!this.running) {
          break;
        }
        await this.processRecord(record);
      }
      this.emit('idle');
    } finally {
      this.processing = false;
      this.notifyIdle();
    }
  }

  private async processRecord(record: AlertQueueRecord): Promise<void> {
    await this.databaseManager.markAlertAttempt(record.id);

    try {
      await this.alertService.sendCvdAlert(record.payload);
      await this.databaseManager.markAlertProcessed(record.id);
      this.emit('alertSent', record);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      logger.error('Failed to dispatch alert payload', {
        alertId: record.id,
        symbol: record.payload.symbol,
        error: message,
      });
      await this.databaseManager.markAlertFailure(record.id, message);

      if (record.attemptCount + 1 >= this.maxAttempts) {
        await this.databaseManager.markAlertProcessed(record.id, { clearError: false });
      }

      this.emit('alertFailed', {
        record,
        error: error instanceof Error ? error : new Error(message),
      });
    }
  }

  private waiters: Array<() => void> = [];

  private notifyIdle(): void {
    while (this.waiters.length > 0) {
      const resolve = this.waiters.shift();
      resolve?.();
    }
  }

  private waitForIdle(): Promise<void> {
    return new Promise((resolve) => {
      if (!this.processing) {
        resolve();
        return;
      }
      this.waiters.push(resolve);
    });
  }
}
