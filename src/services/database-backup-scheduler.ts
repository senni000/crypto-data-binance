import fs from 'fs';
import path from 'path';
import { logger } from '../utils/logger';
import { IDatabaseManager } from './interfaces';

interface RetentionPolicy {
  dailyDays: number;
  weeklyWeeks: number;
}

export interface DatabaseBackupSchedulerOptions {
  sourcePath: string;
  targetDirectory: string;
  intervalMs: number;
  databaseManager: IDatabaseManager;
  retentionPolicy?: RetentionPolicy;
}

const DEFAULT_RETENTION: RetentionPolicy = {
  dailyDays: 7,
  weeklyWeeks: 1,
};

const BACKUP_PREFIX = 'binance_data_';
const BACKUP_EXTENSION = '.sqlite';

export class DatabaseBackupScheduler {
  private timer: NodeJS.Timeout | null = null;
  private running = false;

  constructor(private readonly options: DatabaseBackupSchedulerOptions) {}

  start(): void {
    if (this.timer) {
      return;
    }
    logger.info('Starting database backup scheduler', {
      intervalMs: this.options.intervalMs,
      targetDirectory: this.options.targetDirectory,
    });
    this.schedule(0);
  }

  stop(): void {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }

  private schedule(delayMs: number): void {
    this.timer = setTimeout(() => {
      void this.run();
    }, delayMs);
  }

  private async run(): Promise<void> {
    if (this.running) {
      logger.warn('Skipping backup run because previous run still in progress');
      this.schedule(this.options.intervalMs);
      return;
    }
    this.running = true;
    try {
      await this.performBackup();
      await this.enforceRetention();
      await this.pruneHistoricalData();
    } catch (error) {
      logger.error('Database backup failed', error);
    } finally {
      this.running = false;
      this.schedule(this.options.intervalMs);
    }
  }

  private async performBackup(): Promise<void> {
    const { sourcePath, targetDirectory } = this.options;
    await fs.promises.access(sourcePath, fs.constants.R_OK);
    await fs.promises.mkdir(targetDirectory, { recursive: true });

    const timestamp = new Date().toISOString().replace(/[-:]/g, '').replace(/\.\d+Z$/, 'Z');
    const backupFilename = `${BACKUP_PREFIX}${timestamp}${BACKUP_EXTENSION}`;
    const backupPath = path.join(targetDirectory, backupFilename);

    await fs.promises.copyFile(sourcePath, backupPath);
    const stats = await fs.promises.stat(backupPath);
    logger.info('Database backup created', {
      backupPath,
      size: stats.size,
      modifiedAt: stats.mtime.toISOString(),
    });
  }

  private async enforceRetention(): Promise<void> {
    const retention = this.options.retentionPolicy ?? DEFAULT_RETENTION;
    const files = await this.listBackupFiles();
    const now = Date.now();
    const dailyThreshold = now - retention.dailyDays * 24 * 60 * 60 * 1000;
    const weeklyThreshold = now - retention.weeklyWeeks * 7 * 24 * 60 * 60 * 1000;

    const toDelete: string[] = [];
    const weeklyKeeper = new Map<string, { path: string; timestamp: number }>();

    for (const file of files) {
      const timestamp = this.extractTimestamp(file);
      if (!timestamp) {
        continue;
      }
      const absolutePath = path.join(this.options.targetDirectory, file);
      if (timestamp >= dailyThreshold) {
        continue;
      }
      if (timestamp < weeklyThreshold) {
        toDelete.push(absolutePath);
        continue;
      }

      const weekKey = this.getIsoWeekKey(new Date(timestamp));
      const existing = weeklyKeeper.get(weekKey);
      if (!existing || existing.timestamp < timestamp) {
        if (existing) {
          toDelete.push(existing.path);
        }
        weeklyKeeper.set(weekKey, { path: absolutePath, timestamp });
      } else {
        toDelete.push(absolutePath);
      }
    }

    for (const filePath of toDelete) {
      try {
        await fs.promises.unlink(filePath);
        logger.info('Removed expired backup', { filePath });
      } catch (error) {
        logger.warn('Failed to remove expired backup', { filePath, error });
      }
    }
  }

  private async pruneHistoricalData(): Promise<void> {
    const now = Date.now();
    const sevenDaysAgo = now - 7 * 24 * 60 * 60 * 1000;
    try {
      await this.options.databaseManager.pruneDataBefore('1m', sevenDaysAgo);
      await this.options.databaseManager.pruneDataBefore('30m', sevenDaysAgo);
      await this.options.databaseManager.pruneDataBefore('1d', sevenDaysAgo);
      await this.options.databaseManager.pruneTopTraderDataBefore(sevenDaysAgo);
    } catch (error) {
      logger.warn('Failed to prune historical data', error);
    }
  }

  private async listBackupFiles(): Promise<string[]> {
    const entries = await fs.promises.readdir(this.options.targetDirectory, { withFileTypes: true });
    return entries
      .filter((entry) => entry.isFile() && entry.name.startsWith(BACKUP_PREFIX) && entry.name.endsWith(BACKUP_EXTENSION))
      .map((entry) => entry.name)
      .sort();
  }

  private extractTimestamp(filename: string): number | null {
    const match = filename.match(/^binance_data_(\d{8}T\d{6}Z)\.sqlite$/);
    if (!match) {
      return null;
    }
    const [, timestamp] = match;
    if (!timestamp) {
      return null;
    }
    const parsed = Date.parse(timestamp);
    return Number.isNaN(parsed) ? null : parsed;
  }

  private getIsoWeekKey(date: Date): string {
    const target = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
    const dayNum = target.getUTCDay() || 7;
    target.setUTCDate(target.getUTCDate() + 4 - dayNum);
    const yearStart = new Date(Date.UTC(target.getUTCFullYear(), 0, 1));
    const weekNo = Math.ceil(((target.getTime() - yearStart.getTime()) / 86400000 + 1) / 7);
    return `${target.getUTCFullYear()}-W${weekNo.toString().padStart(2, '0')}`;
  }
}
