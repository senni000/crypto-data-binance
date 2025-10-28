import fs from 'fs';
import path from 'path';
import axios from 'axios';
import { BinanceRestClient } from '../../services/binance-rest-client';
import { IRateLimiter } from '../../services/interfaces';

jest.mock('axios');

const mockedAxios = axios as jest.Mocked<typeof axios>;

const loadFixture = (name: string) => {
  const file = path.join(__dirname, 'fixtures', name);
  return JSON.parse(fs.readFileSync(file, 'utf-8'));
};

const fixtures = {
  spotKlines: loadFixture('klines_spot_30m.json'),
  usdmKlines: loadFixture('klines_usdm_30m.json'),
  coinmKlines: loadFixture('klines_coinm_30m.json'),
  topTraderPositions: loadFixture('top_trader_positions.json'),
  topTraderAccounts: loadFixture('top_trader_accounts.json'),
};

describe('BinanceRestClient', () => {
  let rateLimiter: IRateLimiter;

  beforeEach(() => {
    rateLimiter = {
      registerEndpoint: jest.fn(),
      schedule: jest.fn((_, task: () => Promise<any>) => task()),
      getUsageSnapshot: jest.fn(() => ({ endpoints: [] })),
    };

    mockedAxios.create.mockImplementation((config) => {
      const baseURL = config?.baseURL as string;
      const get = jest.fn((url: string) => {
        if (baseURL.includes('api.binance.com') && url === '/api/v3/klines') {
          return Promise.resolve({ data: fixtures.spotKlines });
        }
        if (baseURL.includes('fapi.binance.com')) {
          if (url === '/fapi/v1/klines') {
            return Promise.resolve({ data: fixtures.usdmKlines });
          }
          if (url === '/futures/data/topLongShortPositionRatio') {
            return Promise.resolve({ data: fixtures.topTraderPositions });
          }
          if (url === '/futures/data/topLongShortAccountRatio') {
            return Promise.resolve({ data: fixtures.topTraderAccounts });
          }
        }
        if (baseURL.includes('dapi.binance.com') && url === '/dapi/v1/klines') {
          return Promise.resolve({ data: fixtures.coinmKlines });
        }
        throw new Error(`Unexpected request ${baseURL}${url}`);
      });
      return { get } as any;
    });
  });

  afterEach(() => {
    jest.resetAllMocks();
  });

  it('fetches and transforms spot klines', async () => {
    const client = new BinanceRestClient(rateLimiter, {
      spotBaseUrl: 'https://api.binance.com',
      usdMBaseUrl: 'https://fapi.binance.com',
      coinMBaseUrl: 'https://dapi.binance.com',
      timeout: 10_000,
    });

    const result = await client.fetchKlines('BTCUSDT', '30m', 'SPOT', 1_700_000_000_000);
    expect(result).toHaveLength(fixtures.spotKlines.length);
    expect(result[0]).toBeDefined();
    expect(result[0]!).toMatchObject({
      symbol: 'BTCUSDT',
      interval: '30m',
    });
    expect(typeof result[0]!.open).toBe('number');
    expect(typeof result[0]!.volume).toBe('number');
  });

  it('fetches futures klines for COIN-M market', async () => {
    const client = new BinanceRestClient(rateLimiter, {
      spotBaseUrl: 'https://api.binance.com',
      usdMBaseUrl: 'https://fapi.binance.com',
      coinMBaseUrl: 'https://dapi.binance.com',
      timeout: 10_000,
    });

    const result = await client.fetchKlines('BTCUSD_PERP', '30m', 'COIN-M');
    expect(result[0]).toBeDefined();
    expect(result[0]!.symbol).toBe('BTCUSD_PERP');
    expect(result[0]!.interval).toBe('30m');
  });

  it('fetches top trader ratios with numeric conversions', async () => {
    const client = new BinanceRestClient(rateLimiter, {
      spotBaseUrl: 'https://api.binance.com',
      usdMBaseUrl: 'https://fapi.binance.com',
      coinMBaseUrl: 'https://dapi.binance.com',
      timeout: 10_000,
    });

    const positions = await client.fetchTopTraderPositions('BTCUSDT');
    expect(positions).not.toHaveLength(0);
    expect(positions[0]).toBeDefined();
    expect(typeof positions[0]!.longShortRatio).toBe('number');
    expect(typeof positions[0]!.timestamp).toBe('number');

    const accounts = await client.fetchTopTraderAccounts('BTCUSDT');
    expect(accounts).not.toHaveLength(0);
    expect(accounts[0]).toBeDefined();
    expect(typeof accounts[0]!.longAccount).toBe('number');
  });
});
