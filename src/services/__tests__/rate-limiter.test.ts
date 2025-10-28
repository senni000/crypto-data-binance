import { RateLimiter } from '../../services/rate-limiter';

describe('RateLimiter', () => {
  afterEach(() => {
    jest.useRealTimers();
  });

  it('queues requests when tokens are exhausted', async () => {
    jest.useFakeTimers();
    const limiter = new RateLimiter();
    limiter.registerEndpoint('queue-test', 1, 1000);

    const executionOrder: number[] = [];

    const first = limiter.schedule({ identifier: 'queue-test', weight: 1 }, async () => {
      executionOrder.push(1);
      return 'first';
    });

    const second = limiter.schedule({ identifier: 'queue-test', weight: 1 }, async () => {
      executionOrder.push(2);
      return 'second';
    });

    await Promise.resolve();
    expect(executionOrder).toEqual([1]);

    jest.advanceTimersByTime(1000);
    await Promise.resolve();

    const [firstResult, secondResult] = await Promise.all([first, second]);
    expect(firstResult).toBe('first');
    expect(secondResult).toBe('second');
    expect(executionOrder).toEqual([1, 2]);
  });

  it('retries requests after rate limit errors with backoff', async () => {
    jest.useFakeTimers();
    const randomSpy = jest.spyOn(Math, 'random').mockReturnValue(0);
    const limiter = new RateLimiter();
    limiter.registerEndpoint('retry-test', 1, 1000);

    let attempts = 0;
    const promise = limiter.schedule({ identifier: 'retry-test', weight: 1 }, async () => {
      attempts += 1;
      if (attempts === 1) {
        const error: any = new Error('rate limited');
        error.response = { status: 429 };
        throw error;
      }
      return 'ok';
    });

    await Promise.resolve();
    expect(attempts).toBe(1);

    await jest.advanceTimersByTimeAsync(1000);
    await Promise.resolve();

    await expect(promise).resolves.toBe('ok');
    expect(attempts).toBe(2);
    randomSpy.mockRestore();
  });
});
