import fs from 'fs';
import os from 'os';
import path from 'path';
import sqlite3 from 'sqlite3';
import { DatabaseManager } from '../../services/database';
import {
  AggTrade,
  LiquidationEvent,
  OHLCVData,
  SymbolMetadata,
  TopTraderAccountData,
  TopTraderPositionData,
} from '../../types';

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

  it('should persist agg trades and expose checkpoints', async () => {
    const trades: AggTrade[] = [
      {
        symbol: 'ETHUSDT',
        marketType: 'SPOT',
        tradeId: 101,
        price: 3000.5,
        quantity: 0.12,
        firstTradeId: 100,
        lastTradeId: 101,
        tradeTime: 1_700_000_000_001,
        isBuyerMaker: false,
        isBestMatch: true,
        source: 'rest',
      },
      {
        symbol: 'ETHUSDT',
        marketType: 'SPOT',
        tradeId: 102,
        price: 3001.1,
        quantity: 0.25,
        firstTradeId: 102,
        lastTradeId: 102,
        tradeTime: 1_700_000_000_100,
        isBuyerMaker: true,
        isBestMatch: true,
        source: 'ws',
      },
    ];

    await manager.saveAggTrades(trades);
    await manager.saveAggTrades([trades[1]!]); // duplicate ignored

    const checkpoint = await manager.getLastAggTradeCheckpoint('ETHUSDT', 'SPOT');
    expect(checkpoint).toEqual({
      tradeId: 102,
      tradeTime: trades[1]!.tradeTime,
    });
  });

  it('should persist liquidation events and ignore duplicates', async () => {
    const now = Date.now();
    const base: LiquidationEvent = {
      eventId: 'USDT-M:liquidation-1',
      symbol: 'BTCUSDT',
      marketType: 'USDT-M',
      side: 'sell',
      orderType: 'MARKET',
      timeInForce: 'GTC',
      status: 'FILLED',
      orderId: '123456',
      price: 25_000,
      averagePrice: 25_010,
      lastFilledPrice: 25_000,
      originalQuantity: 10,
      filledQuantity: 10,
      lastFilledQuantity: 10,
      eventTime: now,
      tradeTime: now,
      isMaker: false,
      reduceOnly: true,
      createdAt: now,
    };

    await manager.saveLiquidationEvents([base]);
    await manager.saveLiquidationEvents([
      {
        ...base,
        price: 26_000,
        createdAt: now + 1,
      },
    ]);

    await new Promise<void>((resolve, reject) => {
      const db = new sqlite3.Database(dbPath, (error) => {
        if (error) {
          reject(error);
          return;
        }

        db.get(
          'SELECT COUNT(*) as count, MAX(price) as max_price FROM liquidation_events',
          (err, row: { count: number; max_price: number }) => {
            db.close();
            if (err) {
              reject(err);
              return;
            }

            expect(row.count).toBe(1);
            expect(row.max_price).toBe(base.price);
            resolve();
          }
        );
      });
    });
  });
});
