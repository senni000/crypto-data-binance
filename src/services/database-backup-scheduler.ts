import fs from 'fs';
import path from 'path';
import { logger } from '../utils/logger';

export interface DatabaseBackupSchedulerOptions {
  sourcePath: string;
  targetPath: string;
  intervalMs: number;
}

/**
 * Periodically copies the main SQLite database to a backup location.
 */
export class DatabaseBackupScheduler {
  private timer: NodeJS.Timeout | null = null;
  private isRunning = false;

  constructor(private readonly options: DatabaseBackupSchedulerOptions) {}

  start(): void {
    if (this.timer) {
      return;
    }

    logger.info('Starting database backup scheduler', {
      intervalMs: this.options.intervalMs,
      targetPath: this.options.targetPath,
    });

    this.scheduleNextRun(0);
  }

  stop(): void {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }

  private scheduleNextRun(delayMs?: number): void {
    const delay = delayMs ?? this.options.intervalMs;
    this.timer = setTimeout(() => {
      void this.run();
    }, delay);
  }

  private async run(): Promise<void> {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    if (this.isRunning) {
      logger.warn('Database backup skipped because previous run is still in progress');
      this.scheduleNextRun();
      return;
    }

    this.isRunning = true;
    try {
      await this.performBackup();
    } catch (error) {
      logger.error('Database backup failed', error);
    } finally {
      this.isRunning = false;
      this.scheduleNextRun();
    }
  }

  private async performBackup(): Promise<void> {
    const { sourcePath, targetPath } = this.options;

    if (sourcePath === targetPath) {
      logger.warn('Database backup skipped because source and target paths are identical', {
        sourcePath,
      });
      return;
    }

    try {
      await fs.promises.access(sourcePath, fs.constants.R_OK);
    } catch (error) {
      logger.warn('Database backup skipped because source file is not accessible', {
        sourcePath,
        error,
      });
      return;
    }

    const targetDir = path.dirname(targetPath);
    await fs.promises.mkdir(targetDir, { recursive: true });

    await fs.promises.copyFile(sourcePath, targetPath);
    const stats = await fs.promises.stat(targetPath);

    logger.info('Database backup completed', {
      targetPath,
      size: stats.size,
      modifiedAt: stats.mtime.toISOString(),
    });
  }
}
