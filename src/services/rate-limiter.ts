import { IRateLimiter, RateLimiterRequest, RateLimiterUsageSnapshot } from './interfaces';
import { logger } from '../utils/logger';

interface NormalizedRequest {
  weight: number;
  identifier: string;
  priority: number;
}

interface QueueItem {
  request: NormalizedRequest;
  task: () => Promise<unknown>;
  resolve: (value: unknown) => void;
  reject: (reason?: unknown) => void;
  attempt: number;
  nextAttemptAt: number;
}

interface EndpointState {
  key: string;
  capacity: number;
  tokens: number;
  refillIntervalMs: number;
  lastRefill: number;
  queue: QueueItem[];
  timer: NodeJS.Timeout | null;
}

const MAX_BACKOFF_MS = 60_000;

export class RateLimiter implements IRateLimiter {
  private readonly endpoints = new Map<string, EndpointState>();

  registerEndpoint(key: string, capacity: number, intervalMs: number): void {
    if (this.endpoints.has(key)) {
      return;
    }
    this.endpoints.set(key, {
      key,
      capacity,
      tokens: capacity,
      refillIntervalMs: intervalMs,
      lastRefill: Date.now(),
      queue: [],
      timer: null,
    });
  }

  schedule<T>(request: RateLimiterRequest, task: () => Promise<T>): Promise<T> {
    if (!request.identifier) {
      throw new Error('RateLimiter.schedule requires request.identifier to map endpoint');
    }

    const endpoint = this.endpoints.get(request.identifier);
    if (!endpoint) {
      throw new Error(`Rate limiter endpoint not registered: ${request.identifier}`);
    }

    const normalized = this.normalizeRequest(request);

    return new Promise<T>((resolve, reject) => {
      const queueItem: QueueItem = {
        request: normalized,
        task: async () => task(),
        resolve: (value) => resolve(value as T),
        reject,
        attempt: 0,
        nextAttemptAt: Date.now(),
      };

      endpoint.queue.push(queueItem);
      endpoint.queue.sort((a, b) => a.request.priority - b.request.priority);

      this.process(endpoint);
    });
  }

  getUsageSnapshot(): RateLimiterUsageSnapshot {
    return {
      endpoints: Array.from(this.endpoints.values()).map((endpoint) => ({
        key: endpoint.key,
        availableTokens: endpoint.tokens,
        capacity: endpoint.capacity,
        refillIntervalMs: endpoint.refillIntervalMs,
        queueLength: endpoint.queue.length,
      })),
    };
  }

  private process(endpoint: EndpointState): void {
    this.refillTokens(endpoint);

    if (endpoint.queue.length === 0) {
      return;
    }

    const now = Date.now();
    const nextItem = endpoint.queue[0];
    if (!nextItem) {
      return;
    }

    if (nextItem.nextAttemptAt > now) {
      this.scheduleTimer(endpoint, nextItem.nextAttemptAt - now);
      return;
    }

    if (endpoint.tokens < nextItem.request.weight) {
      const msUntilRefill = endpoint.refillIntervalMs - (now - endpoint.lastRefill);
      this.scheduleTimer(endpoint, Math.max(msUntilRefill, 10));
      return;
    }

    const item = endpoint.queue.shift();
    if (!item) {
      return;
    }

    endpoint.tokens -= item.request.weight;

    item
      .task()
      .then((result) => {
        item.resolve(result);
        this.process(endpoint);
      })
      .catch((error) => {
        if (this.isRateLimitError(error)) {
          item.attempt += 1;
          const backoff = this.calculateBackoff(item.attempt);
          logger.warn('Rate limited request detected, retrying', {
            endpoint: endpoint.key,
            backoffMs: backoff,
            attempt: item.attempt,
          });
          item.nextAttemptAt = Date.now() + backoff;
          endpoint.queue.push(item);
          endpoint.queue.sort((a, b) => a.request.priority - b.request.priority);
          this.scheduleTimer(endpoint, backoff);
        } else {
          item.reject(error);
        }
        this.process(endpoint);
      });
  }

  private normalizeRequest(request: RateLimiterRequest): NormalizedRequest {
    if (!request.identifier) {
      throw new Error('Request identifier is required');
    }
    return {
      weight: request.weight,
      identifier: request.identifier,
      priority: request.priority ?? 0,
    };
  }

  private refillTokens(endpoint: EndpointState): void {
    const now = Date.now();
    if (now - endpoint.lastRefill >= endpoint.refillIntervalMs) {
      const intervals = Math.floor((now - endpoint.lastRefill) / endpoint.refillIntervalMs);
      endpoint.tokens = Math.min(endpoint.capacity, endpoint.tokens + endpoint.capacity * intervals);
      endpoint.lastRefill += endpoint.refillIntervalMs * intervals;
    }
  }

  private scheduleTimer(endpoint: EndpointState, delay: number): void {
    if (endpoint.timer) {
      return;
    }
    endpoint.timer = setTimeout(() => {
      endpoint.timer = null;
      this.process(endpoint);
    }, delay);
  }

  private isRateLimitError(error: unknown): boolean {
    if (!error || typeof error !== 'object') {
      return false;
    }
    const maybeAxiosError = error as { response?: { status?: number } };
    return maybeAxiosError.response?.status === 429;
  }

  private calculateBackoff(attempt: number): number {
    const base = Math.min(MAX_BACKOFF_MS, 1000 * Math.pow(2, attempt - 1));
    const jitter = Math.random() * 1000;
    return Math.min(MAX_BACKOFF_MS, base + jitter);
  }
}
