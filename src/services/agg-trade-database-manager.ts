import fs from 'fs';
import path from 'path';
import sqlite3 from 'sqlite3';
import { AggTrade } from '../types';
import { IAggTradeDatabaseManager } from './interfaces';

sqlite3.verbose();

interface DatabaseHandle {
  db: sqlite3.Database;
  chain: Promise<void>;
  filePath: string;
}

const SCHEMA_STATEMENTS = [
  `CREATE TABLE IF NOT EXISTS agg_trades (
    symbol TEXT NOT NULL,
    market_type TEXT NOT NULL,
    trade_id INTEGER NOT NULL,
    price REAL NOT NULL,
    quantity REAL NOT NULL,
    first_trade_id INTEGER NOT NULL,
    last_trade_id INTEGER NOT NULL,
    trade_time INTEGER NOT NULL,
    is_buyer_maker INTEGER NOT NULL,
    is_best_match INTEGER NOT NULL,
    source TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, market_type, trade_id)
  )`,
  `CREATE INDEX IF NOT EXISTS idx_agg_trades_time ON agg_trades(symbol, market_type, trade_time)`,
];

export class AggTradeDatabaseManager implements IAggTradeDatabaseManager {
  private readonly handles = new Map<string, DatabaseHandle>();

  constructor(private readonly baseDirectory: string) {}

  async initialize(): Promise<void> {
    await fs.promises.mkdir(this.baseDirectory, { recursive: true });
  }

  async saveAggTrades(asset: string, trades: AggTrade[]): Promise<void> {
    if (trades.length === 0) {
      return;
    }

    const handle = await this.getHandle(asset);
    handle.chain = handle.chain.then(() => this.insertTrades(handle.db, trades));
    await handle.chain;
  }

  async getLastAggTradeCheckpoint(
    asset: string,
    symbol: string,
    marketType: AggTrade['marketType']
  ): Promise<{ tradeId: number; tradeTime: number } | undefined> {
    const handle = await this.getHandle(asset);
    await handle.chain;
    const row = await this.get<{ trade_id: number; trade_time: number }>(
      handle.db,
      `SELECT trade_id, trade_time
         FROM agg_trades
         WHERE symbol = ? AND market_type = ?
         ORDER BY trade_id DESC
         LIMIT 1`,
      [symbol, marketType]
    );
    if (!row) {
      return undefined;
    }
    return {
      tradeId: Number(row.trade_id),
      tradeTime: Number(row.trade_time),
    };
  }

  async close(): Promise<void> {
    const handles = Array.from(this.handles.values());
    this.handles.clear();
    for (const handle of handles) {
      await handle.chain;
      await new Promise<void>((resolve, reject) => {
        handle.db.close((error) => {
          if (error) {
            reject(error);
          } else {
            resolve();
          }
        });
      });
    }
  }

  private async getHandle(asset: string): Promise<DatabaseHandle> {
    const key = asset.toUpperCase();
    const existing = this.handles.get(key);
    if (existing) {
      return existing;
    }

    const filePath = path.join(this.baseDirectory, `${key.toLowerCase()}.sqlite`);
    await fs.promises.mkdir(path.dirname(filePath), { recursive: true });
    const db = await this.openDatabase(filePath);
    await this.configureDatabase(db);
    await this.ensureSchema(db);
    const handle: DatabaseHandle = {
      db,
      chain: Promise.resolve(),
      filePath,
    };
    this.handles.set(key, handle);
    return handle;
  }

  private openDatabase(filePath: string): Promise<sqlite3.Database> {
    return new Promise((resolve, reject) => {
      const db = new sqlite3.Database(filePath, (error) => {
        if (error) {
          reject(error);
        } else {
          resolve(db);
        }
      });
    });
  }

  private async insertTrades(db: sqlite3.Database, trades: AggTrade[]): Promise<void> {
    await this.exec(db, 'BEGIN IMMEDIATE');
    try {
      const sql = `
        INSERT OR IGNORE INTO agg_trades (
          symbol, market_type, trade_id, price, quantity,
          first_trade_id, last_trade_id, trade_time,
          is_buyer_maker, is_best_match, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `;
      for (const trade of trades) {
        await this.run(db, sql, [
          trade.symbol,
          trade.marketType,
          trade.tradeId,
          trade.price,
          trade.quantity,
          trade.firstTradeId,
          trade.lastTradeId,
          trade.tradeTime,
          trade.isBuyerMaker ? 1 : 0,
          trade.isBestMatch ? 1 : 0,
          trade.source,
        ]);
      }
      await this.exec(db, 'COMMIT');
    } catch (error) {
      await this.exec(db, 'ROLLBACK');
      throw error;
    }
  }

  private async ensureSchema(db: sqlite3.Database): Promise<void> {
    for (const statement of SCHEMA_STATEMENTS) {
      await this.exec(db, statement);
    }
  }

  private async configureDatabase(db: sqlite3.Database): Promise<void> {
    await this.exec(db, 'PRAGMA journal_mode = WAL');
    await this.exec(db, 'PRAGMA synchronous = NORMAL');
    await this.exec(db, 'PRAGMA busy_timeout = 5000');
  }

  private exec(db: sqlite3.Database, sql: string): Promise<void> {
    return new Promise((resolve, reject) => {
      db.exec(sql, (error) => {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  }

  private run(db: sqlite3.Database, sql: string, params: unknown[]): Promise<void> {
    return new Promise((resolve, reject) => {
      db.run(sql, params, (error) => {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  }

  private get<T>(
    db: sqlite3.Database,
    sql: string,
    params: unknown[]
  ): Promise<T | undefined> {
    return new Promise((resolve, reject) => {
      db.get(sql, params, (error, row) => {
        if (error) {
          reject(error);
        } else {
          resolve((row as T) ?? undefined);
        }
      });
    });
  }
}

