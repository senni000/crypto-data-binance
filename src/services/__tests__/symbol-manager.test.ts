import fs from 'fs';
import os from 'os';
import path from 'path';
import axios from 'axios';
import { DatabaseManager } from '../../services/database';
import { SymbolManager } from '../../services/symbol-manager';
import { IRateLimiter } from '../../services/interfaces';
import { SymbolMetadata } from '../../types';

jest.mock('axios');

const mockedAxios = axios as jest.Mocked<typeof axios>;

const loadFixture = (name: string) => {
  const file = path.join(__dirname, 'fixtures', name);
  const raw = fs.readFileSync(file, 'utf-8');
  return JSON.parse(raw);
};

const fixtureMap: Record<string, any> = {
  'https://api.binance.com': loadFixture('exchangeInfo_spot.json'),
  'https://fapi.binance.com': loadFixture('exchangeInfo_usdm.json'),
  'https://dapi.binance.com': loadFixture('exchangeInfo_coinm.json'),
};

describe('SymbolManager', () => {
  let db: DatabaseManager;
  let rateLimiter: IRateLimiter;
  let dbPath: string;

  beforeEach(async () => {
    dbPath = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'symbols-')), 'symbols.sqlite');
    db = new DatabaseManager(dbPath);
    await db.initialize();
    await db.runMigrations();

    rateLimiter = {
      registerEndpoint: jest.fn(),
      schedule: jest.fn((_, task: () => Promise<any>) => task()),
      getUsageSnapshot: jest.fn(() => ({ endpoints: [] })),
    };

    mockedAxios.create.mockImplementation((config) => {
      const baseURL = (config?.baseURL as string) ?? '';
      const data = fixtureMap[baseURL];
      if (!data) {
        throw new Error(`Unexpected baseURL in mock: ${baseURL}`);
      }
      return {
        get: jest.fn().mockResolvedValue({ data }),
      } as any;
    });
  });

  afterEach(() => {
    jest.resetAllMocks();
    try {
      if (fs.existsSync(dbPath)) {
        fs.unlinkSync(dbPath);
      }
      const dir = path.dirname(dbPath);
      if (fs.existsSync(dir)) {
        fs.rmdirSync(dir);
      }
    } catch {
      // ignore
    }
  });

  it('updates symbols from Binance exchange info and marks missing as inactive', async () => {
    const preExisting: SymbolMetadata = {
      symbol: 'LTCUSDT',
      baseAsset: 'LTC',
      quoteAsset: 'USDT',
      marketType: 'SPOT',
      status: 'ACTIVE',
      onboardDate: Date.now() - 1000,
      filters: {},
      updatedAt: Date.now() - 1000,
    };

    await db.upsertSymbols([preExisting]);

    const manager = new SymbolManager(db, rateLimiter, {
      spotBaseUrl: 'https://api.binance.com',
      usdMBaseUrl: 'https://fapi.binance.com',
      coinMBaseUrl: 'https://dapi.binance.com',
      symbolUpdateHourUtc: 1,
    });

    const updatedEvent = new Promise<SymbolMetadata[]>((resolve) => {
      manager.once('updated', (symbols) => resolve(symbols));
    });

    await manager.updateSymbols();
    const updatedSymbols = await updatedEvent;

    expect(updatedSymbols.length).toBeGreaterThan(0);
    const spotSymbols = await db.listActiveSymbols('SPOT');
    const hasBtc = spotSymbols.some((s) => s.symbol === 'BTCUSDT');
    expect(hasBtc).toBe(true);

    const allSymbols = await db.listAllSymbols();
    const ltc = allSymbols.find((s) => s.symbol === 'LTCUSDT' && s.marketType === 'SPOT');
    expect(ltc?.status).toBe('INACTIVE');
  });
});
