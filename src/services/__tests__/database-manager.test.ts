import fs from 'fs';
import os from 'os';
import path from 'path';
import { DatabaseManager } from '../../services/database';
import { OHLCVData, SymbolMetadata, TopTraderAccountData, TopTraderPositionData } from '../../types';

const createTempPath = (): string => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'binance-db-'));
  return path.join(dir, 'collector.sqlite');
};

describe('DatabaseManager', () => {
  let dbPath: string;
  let manager: DatabaseManager;

  beforeEach(async () => {
    dbPath = createTempPath();
    manager = new DatabaseManager(dbPath);
    await manager.initialize();
    await manager.runMigrations();
  });

  afterEach(() => {
    try {
      if (fs.existsSync(dbPath)) {
        fs.unlinkSync(dbPath);
      }
      const dir = path.dirname(dbPath);
      if (fs.existsSync(dir)) {
        fs.rmdirSync(dir);
      }
    } catch {
      // best effort cleanup
    }
  });

  it('should upsert and list active symbols', async () => {
    const symbols: SymbolMetadata[] = [
      {
        symbol: 'BTCUSDT',
        baseAsset: 'BTC',
        quoteAsset: 'USDT',
        marketType: 'SPOT',
        status: 'ACTIVE',
        onboardDate: Date.now(),
        filters: {},
        updatedAt: Date.now(),
      },
      {
        symbol: 'ETHUSDT',
        baseAsset: 'ETH',
        quoteAsset: 'USDT',
        marketType: 'USDT-M',
        status: 'ACTIVE',
        onboardDate: Date.now(),
        contractType: 'PERPETUAL',
        deliveryDate: Date.now() + 1000,
        filters: { tickSize: 0.1, stepSize: 0.001, minNotional: 10 },
        updatedAt: Date.now(),
      },
    ];

    await manager.upsertSymbols(symbols);
    const active = await manager.listActiveSymbols();
    expect(active).toHaveLength(2);

    const usdtm = await manager.listActiveSymbols('USDT-M');
    expect(usdtm).toHaveLength(1);
    expect(usdtm[0]!.symbol).toBe('ETHUSDT');
  });

  it('should store OHLCV data per interval and report latest timestamps', async () => {
    const base: OHLCVData = {
      symbol: 'BTCUSDT',
      interval: '30m',
      openTime: 1_700_000_000_000,
      closeTime: 1_700_000_000_000 + 1_800_000,
      open: 10,
      high: 12,
      low: 9,
      close: 11,
      volume: 120,
      quoteVolume: 150,
      trades: 500,
    };

    await manager.saveOHLCVBatch([base, { ...base, openTime: base.openTime + 1_800_000, closeTime: base.closeTime + 1_800_000 }]);
    const timestamps = await manager.getLastOHLCVTimestamps('30m');
    expect(timestamps['BTCUSDT']).toBe(base.openTime + 1_800_000);
  });

  it('should persist top trader data and prune historical entries', async () => {
    const now = Date.now();
    const positions: TopTraderPositionData[] = [
      {
        symbol: 'BTCUSDT',
        timestamp: now - 60_000,
        longShortRatio: 1.5,
        longAccount: 55,
        shortAccount: 45,
        longPosition: 600,
        shortPosition: 400,
      },
      {
        symbol: 'ETHUSDT',
        timestamp: now - 120_000,
        longShortRatio: 1.2,
        longAccount: 52,
        shortAccount: 48,
        longPosition: 320,
        shortPosition: 280,
      },
    ];

    const accounts: TopTraderAccountData[] = positions.map(({ symbol, timestamp, longShortRatio, longAccount, shortAccount }) => ({
      symbol,
      timestamp,
      longShortRatio,
      longAccount,
      shortAccount,
    }));

    await manager.saveTopTraderPositions(positions);
    await manager.saveTopTraderAccounts(accounts);

    const firstPosition = positions[0]!;
    const secondPosition = positions[1]!;

    const last = await manager.getLastTopTraderTimestamp();
    expect(last).toBe(firstPosition.timestamp);

    await manager.pruneTopTraderDataBefore(now - 90_000);
    const latest = await manager.getLastTopTraderTimestamp();
    expect(latest).toBeGreaterThan(secondPosition.timestamp);
  });
});
