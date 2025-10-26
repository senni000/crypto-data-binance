import { IDatabaseManager } from './interfaces';
import { AlertManager } from './alert-manager';
import { logger } from '../utils/logger';
import { CVDData } from '../types';

export interface CVDReportSchedulerOptions {
  intervalMs?: number;
  lookbackMinutes?: number;
  maxPoints?: number;
  timezone?: string;
}

/**
 * Scheduler that generates an hourly CVD chart and sends it to Discord.
 */
export class CVDReportScheduler {
  private readonly databaseManager: IDatabaseManager;
  private readonly alertManager: AlertManager;
  private readonly intervalMs: number;
  private readonly lookbackMinutes: number;
  private readonly maxPoints: number;
  private readonly timezone: string;
  private timer: NodeJS.Timeout | null = null;
  private isRunning = false;

  constructor(
    databaseManager: IDatabaseManager,
    alertManager: AlertManager,
    options: CVDReportSchedulerOptions = {}
  ) {
    this.databaseManager = databaseManager;
    this.alertManager = alertManager;
    this.intervalMs = options.intervalMs ?? 60 * 60 * 1000; // default 1 hour
    this.lookbackMinutes = options.lookbackMinutes ?? 60;
    this.maxPoints = options.maxPoints ?? 60;
    this.timezone = options.timezone ?? 'Asia/Tokyo';
  }

  /**
   * Start the scheduler.
   */
  start(): void {
    if (this.timer) {
      return;
    }

    logger.info('Starting CVD report scheduler');
    this.scheduleNextRun();
  }

  /**
   * Stop the scheduler.
   */
  stop(): void {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }

  private scheduleNextRun(): void {
    const delay = this.calculateDelayToNextBoundary();
    this.timer = setTimeout(() => {
      void this.handleRun();
    }, delay);
  }

  private calculateDelayToNextBoundary(): number {
    const now = Date.now();
    const next = Math.ceil(now / this.intervalMs) * this.intervalMs;
    return Math.max(next - now, 1000);
  }

  private async handleRun(): Promise<void> {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    if (this.isRunning) {
      logger.warn('CVD report generation skipped because previous run is still in progress');
      this.scheduleNextRun();
      return;
    }

    this.isRunning = true;
    try {
      await this.generateAndSendReport();
    } catch (error) {
      logger.error('Failed to generate CVD report', error);
    } finally {
      this.isRunning = false;
      this.scheduleNextRun();
    }
  }

  private async generateAndSendReport(): Promise<void> {
    const endTime = Date.now();
    const startTime = endTime - this.lookbackMinutes * 60 * 1000;

    const cvdData = await this.databaseManager.getCVDDataSince(startTime);
    if (cvdData.length < 2) {
      logger.warn('Skipping CVD report because data points are insufficient', {
        points: cvdData.length,
      });
      return;
    }

    const chartData = this.prepareChartData(cvdData, startTime);
    if (chartData.labels.length < 2) {
      logger.warn('Skipping CVD report because prepared chart data is insufficient', chartData);
      return;
    }

    if (chartData.values.length === 0) {
      logger.warn('Skipping CVD report because value series is empty');
      return;
    }

    const chartUrl = this.buildChartUrl(chartData.labels, chartData.values);
    const description = this.buildDescription(chartData.values, startTime, endTime);

    await this.alertManager.sendDiscordEmbed({
      title: `BTC CVD (${this.lookbackMinutes}分)`,
      description,
      imageUrl: chartUrl,
    });

    logger.info('Dispatched hourly CVD chart to Discord', {
      points: chartData.labels.length,
    });
  }

  private prepareChartData(
    data: CVDData[],
    startTime: number
  ): { labels: string[]; values: number[] } & { min: number; max: number; last: number } {
    const bucketed = new Map<number, number>();

    for (const entry of data) {
      if (entry.timestamp < startTime) {
        continue;
      }
      const bucket = Math.floor(entry.timestamp / 60000) * 60000;
      const existing = bucketed.get(bucket);
      if (existing === undefined || entry.timestamp >= bucket) {
        bucketed.set(bucket, entry.cvdValue);
      }
    }

    let sorted = Array.from(bucketed.entries()).sort((a, b) => a[0] - b[0]);

    if (sorted.length === 0) {
      return { labels: [], values: [], min: 0, max: 0, last: 0 };
    }

    if (sorted.length > this.maxPoints) {
      const step = Math.ceil(sorted.length / this.maxPoints);
      const reduced: Array<[number, number]> = [];
      for (let i = 0; i < sorted.length; i += step) {
        const point = sorted[i];
        if (point) {
          reduced.push(point);
        }
      }
      const lastPoint = sorted[sorted.length - 1]!;
      const lastReduced = reduced[reduced.length - 1];
      if (!lastReduced || lastReduced[0] !== lastPoint[0]) {
        reduced.push(lastPoint);
      }
      sorted = reduced;
    }

    const formatter = new Intl.DateTimeFormat('ja-JP', {
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
      timeZone: this.timezone,
    });

    const labels = sorted.map(([timestamp]) => formatter.format(new Date(timestamp)));
    const values = sorted.map(([, value]) => value);

    const min = Math.min(...values);
    const max = Math.max(...values);
    const last = values[values.length - 1] ?? 0;

    return { labels, values, min, max, last };
  }

  private buildChartUrl(labels: string[], values: number[]): string {
    const chartConfig = {
      type: 'line',
      data: {
        labels,
        datasets: [
          {
            label: 'CVD',
            data: values,
            fill: false,
            borderColor: '#4bc0c0',
            backgroundColor: 'rgba(75, 192, 192, 0.2)',
            tension: 0.2,
            pointRadius: 0,
          },
        ],
      },
      options: {
        responsive: true,
        plugins: {
          legend: {
            display: false,
          },
          title: {
            display: true,
            text: 'BTC CVD',
          },
        },
        scales: {
          x: {
            title: {
              display: true,
              text: '時間',
            },
          },
          y: {
            title: {
              display: true,
              text: 'CVD',
            },
          },
        },
      },
    };

    const encodedConfig = encodeURIComponent(JSON.stringify(chartConfig));
    return `https://quickchart.io/chart?width=900&height=400&devicePixelRatio=2&format=png&c=${encodedConfig}`;
  }

  private buildDescription(values: number[], startTime: number, endTime: number): string {
    if (values.length === 0) {
      return 'CVDデータが存在しません。';
    }

    const min = Math.min(...values);
    const max = Math.max(...values);
    const last = values[values.length - 1] ?? 0;

    const formatter = new Intl.DateTimeFormat('ja-JP', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
      timeZone: this.timezone,
    });

    const startText = formatter.format(new Date(startTime));
    const endText = formatter.format(new Date(endTime));

    return [
      `期間: ${startText} 〜 ${endText}`,
      `最新: ${last.toFixed(2)}`,
      `高値: ${max.toFixed(2)}`,
      `安値: ${min.toFixed(2)}`,
    ].join('\n');
  }
}
