/**
 * SQLiteデータベース管理
 * - スキーマ初期化とマイグレーション
 * - トランザクションベースのバルク書き込み
 * - OHLCV / Top Trader / Symbol メタデータの永続化
 */

import fs from 'fs';
import path from 'path';
import sqlite3 from 'sqlite3';
import {
  MarketType,
  OHLCVData,
  OHLCVTimeframe,
  AggTrade,
  SymbolMetadata,
  TopTraderAccountData,
  TopTraderPositionData,
} from '../types';
import { IDatabaseManager } from './interfaces';

sqlite3.verbose();

interface Migration {
  id: number;
  name: string;
  statements: string[];
}

const MIGRATIONS: Migration[] = [
  {
    id: 1,
    name: 'create_base_tables',
    statements: [
      `CREATE TABLE IF NOT EXISTS schema_migrations (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`,
      `CREATE TABLE IF NOT EXISTS symbols (
        symbol TEXT NOT NULL,
        base_asset TEXT NOT NULL,
        quote_asset TEXT NOT NULL,
        market_type TEXT NOT NULL,
        status TEXT NOT NULL,
        onboard_date INTEGER NOT NULL,
        contract_type TEXT,
        delivery_date INTEGER,
        tick_size REAL,
        step_size REAL,
        min_notional REAL,
        updated_at INTEGER NOT NULL,
        PRIMARY KEY (symbol, market_type)
      )`,
      `CREATE TABLE IF NOT EXISTS ohlcv_1m (
        symbol TEXT NOT NULL,
        open_time INTEGER NOT NULL,
        close_time INTEGER NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL NOT NULL,
        quote_volume REAL NOT NULL,
        trades INTEGER NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (symbol, open_time)
      )`,
      `CREATE TABLE IF NOT EXISTS ohlcv_30m (
        symbol TEXT NOT NULL,
        open_time INTEGER NOT NULL,
        close_time INTEGER NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL NOT NULL,
        quote_volume REAL NOT NULL,
        trades INTEGER NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (symbol, open_time)
      )`,
      `CREATE TABLE IF NOT EXISTS ohlcv_1d (
        symbol TEXT NOT NULL,
        open_time INTEGER NOT NULL,
        close_time INTEGER NOT NULL,
        open REAL NOT NULL,
        high REAL NOT NULL,
        low REAL NOT NULL,
        close REAL NOT NULL,
        volume REAL NOT NULL,
        quote_volume REAL NOT NULL,
        trades INTEGER NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (symbol, open_time)
      )`,
      `CREATE TABLE IF NOT EXISTS top_trader_positions (
        symbol TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        long_short_ratio REAL NOT NULL,
        long_account REAL NOT NULL,
        short_account REAL NOT NULL,
        long_position REAL NOT NULL,
        short_position REAL NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (symbol, timestamp)
      )`,
      `CREATE TABLE IF NOT EXISTS top_trader_accounts (
        symbol TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        long_short_ratio REAL NOT NULL,
        long_account REAL NOT NULL,
        short_account REAL NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (symbol, timestamp)
      )`,
      `CREATE INDEX IF NOT EXISTS idx_symbols_market_status ON symbols(market_type, status)`,
      `CREATE INDEX IF NOT EXISTS idx_ohlcv_1m_open_time ON ohlcv_1m(open_time)`,
      `CREATE INDEX IF NOT EXISTS idx_ohlcv_30m_open_time ON ohlcv_30m(open_time)`,
      `CREATE INDEX IF NOT EXISTS idx_ohlcv_1d_open_time ON ohlcv_1d(open_time)`,
      `CREATE INDEX IF NOT EXISTS idx_top_trader_positions_timestamp ON top_trader_positions(timestamp)`,
      `CREATE INDEX IF NOT EXISTS idx_top_trader_accounts_timestamp ON top_trader_accounts(timestamp)`
    ],
  },
  {
    id: 2,
    name: 'create_agg_trades_table',
    statements: [
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
      `CREATE INDEX IF NOT EXISTS idx_agg_trades_time ON agg_trades(symbol, market_type, trade_time)`
    ],
  },
];

export class DatabaseManager implements IDatabaseManager {
  private db: sqlite3.Database | null = null;
  private transactionChain: Promise<void> = Promise.resolve();

  constructor(private readonly databasePath: string) {}

  async initialize(): Promise<void> {
    await this.ensureDirectory();
    await this.openDatabase();
    await this.configureDatabase();
  }

  async runMigrations(): Promise<void> {
    const db = this.getDb();
    await this.exec('BEGIN');
    try {
      await this.exec(`CREATE TABLE IF NOT EXISTS schema_migrations (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`);
      const appliedMigrations = await this.getAppliedMigrations();
      for (const migration of MIGRATIONS) {
        if (appliedMigrations.has(migration.id)) {
          continue;
        }
        for (const statement of migration.statements) {
          await this.run(statement);
        }
        await this.run(
          'INSERT INTO schema_migrations (id, name) VALUES (?, ?)',
          migration.id,
          migration.name
        );
      }
      await this.exec('COMMIT');
    } catch (error) {
      await this.exec('ROLLBACK');
      throw error;
    } finally {
      db.serialize();
    }
  }

  async upsertSymbols(symbols: SymbolMetadata[]): Promise<void> {
    if (symbols.length === 0) {
      return;
    }

    const sql = `
      INSERT INTO symbols (
        symbol, base_asset, quote_asset, market_type, status,
        onboard_date, contract_type, delivery_date,
        tick_size, step_size, min_notional, updated_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(symbol, market_type) DO UPDATE SET
        base_asset=excluded.base_asset,
        quote_asset=excluded.quote_asset,
        market_type=excluded.market_type,
        status=excluded.status,
        onboard_date=excluded.onboard_date,
        contract_type=excluded.contract_type,
        delivery_date=excluded.delivery_date,
        tick_size=excluded.tick_size,
        step_size=excluded.step_size,
        min_notional=excluded.min_notional,
        updated_at=excluded.updated_at
    `;

    await this.withTransaction(async (db) => {
      for (const symbol of symbols) {
        await this.runSql(db, sql, [
          symbol.symbol,
          symbol.baseAsset,
          symbol.quoteAsset,
          symbol.marketType,
          symbol.status,
          symbol.onboardDate,
          symbol.contractType ?? null,
          symbol.deliveryDate ?? null,
          symbol.filters.tickSize ?? null,
          symbol.filters.stepSize ?? null,
          symbol.filters.minNotional ?? null,
          symbol.updatedAt,
        ]);
      }
    });
  }

  async listActiveSymbols(marketType?: MarketType): Promise<SymbolMetadata[]> {
    const sql = marketType
      ? `SELECT * FROM symbols WHERE status = 'ACTIVE' AND market_type = ?`
      : `SELECT * FROM symbols WHERE status = 'ACTIVE'`;
    const params = marketType ? [marketType] : [];
    const rows = await this.all(sql, params);
    return rows.map(this.mapSymbolRow);
  }

  async listAllSymbols(): Promise<SymbolMetadata[]> {
    const rows = await this.all('SELECT * FROM symbols');
    return rows.map(this.mapSymbolRow);
  }

  async markSymbolsInactive(entries: Array<{ symbol: string; marketType: MarketType }>): Promise<void> {
    if (entries.length === 0) {
      return;
    }

    await this.withTransaction(async (db) => {
      const sql = `UPDATE symbols SET status = 'INACTIVE', updated_at = ? WHERE symbol = ? AND market_type = ?`;
      for (const entry of entries) {
        await this.runSql(db, sql, [Date.now(), entry.symbol, entry.marketType]);
      }
    });
  }

  async saveOHLCVBatch(data: OHLCVData[]): Promise<void> {
    if (data.length === 0) {
      return;
    }

    const grouped = new Map<OHLCVTimeframe, OHLCVData[]>();
    for (const item of data) {
      const list = grouped.get(item.interval) ?? [];
      list.push(item);
      grouped.set(item.interval, list);
    }

    await this.withTransaction(async (db) => {
      for (const [interval, items] of grouped.entries()) {
        const table = this.getOhlcvTable(interval);
        const sql = `
          INSERT OR IGNORE INTO ${table} (
            symbol, open_time, close_time, open, high, low, close,
            volume, quote_volume, trades
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `;
        for (const item of items) {
          await this.runSql(db, sql, [
            item.symbol,
            item.openTime,
            item.closeTime,
            item.open,
            item.high,
            item.low,
            item.close,
            item.volume,
            item.quoteVolume,
            item.trades,
          ]);
        }
      }
    });
  }

  async saveAggTrades(trades: AggTrade[]): Promise<void> {
    if (trades.length === 0) {
      return;
    }

    const sql = `
      INSERT OR IGNORE INTO agg_trades (
        symbol, market_type, trade_id, price, quantity,
        first_trade_id, last_trade_id, trade_time,
        is_buyer_maker, is_best_match, source
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;

    await this.withTransaction(async (db) => {
      for (const trade of trades) {
        await this.runSql(db, sql, [
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
    });
  }

  async saveTopTraderPositions(data: TopTraderPositionData[]): Promise<void> {
    if (data.length === 0) {
      return;
    }

    const sql = `
      INSERT OR REPLACE INTO top_trader_positions (
        symbol, timestamp, long_short_ratio, long_account,
        short_account, long_position, short_position
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
    `;

    await this.withTransaction(async (db) => {
      for (const item of data) {
        await this.runSql(db, sql, [
          item.symbol,
          item.timestamp,
          item.longShortRatio,
          item.longAccount,
          item.shortAccount,
          item.longPosition,
          item.shortPosition,
        ]);
      }
    });
  }

  async saveTopTraderAccounts(data: TopTraderAccountData[]): Promise<void> {
    if (data.length === 0) {
      return;
    }

    const sql = `
      INSERT OR REPLACE INTO top_trader_accounts (
        symbol, timestamp, long_short_ratio, long_account, short_account
      ) VALUES (?, ?, ?, ?, ?)
    `;

    await this.withTransaction(async (db) => {
      for (const item of data) {
        await this.runSql(db, sql, [
          item.symbol,
          item.timestamp,
          item.longShortRatio,
          item.longAccount,
          item.shortAccount,
        ]);
      }
    });
  }

  async pruneDataBefore(interval: OHLCVTimeframe, cutoff: number): Promise<void> {
    const table = this.getOhlcvTable(interval);
    await this.run(`DELETE FROM ${table} WHERE open_time < ?`, cutoff);
  }

  async pruneTopTraderDataBefore(cutoff: number): Promise<void> {
    await this.run('DELETE FROM top_trader_positions WHERE timestamp < ?', cutoff);
    await this.run('DELETE FROM top_trader_accounts WHERE timestamp < ?', cutoff);
  }

  async getLastOHLCVTimestamps(interval: OHLCVTimeframe): Promise<Record<string, number | undefined>> {
    const table = this.getOhlcvTable(interval);
    const rows = await this.all(
      `SELECT symbol, MAX(open_time) as open_time FROM ${table} GROUP BY symbol`
    );
    const result: Record<string, number | undefined> = {};
    for (const row of rows) {
      result[row.symbol] = row.open_time ?? undefined;
    }
    return result;
  }

  async getLastAggTradeCheckpoint(
    symbol: string,
    marketType: AggTrade['marketType']
  ): Promise<{ tradeId: number; tradeTime: number } | undefined> {
    const row = await this.get<{
      trade_id: number;
      trade_time: number;
    }>(
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

  async getLastTopTraderTimestamp(): Promise<number | undefined> {
    const row = await this.get<{ timestamp: number }>(
      `SELECT MAX(timestamp) as timestamp FROM top_trader_positions`
    );
    return row?.timestamp ?? undefined;
  }

  private getOhlcvTable(interval: OHLCVTimeframe): string {
    switch (interval) {
      case '1m':
        return 'ohlcv_1m';
      case '30m':
        return 'ohlcv_30m';
      case '1d':
        return 'ohlcv_1d';
      default:
        throw new Error(`Unsupported interval: ${interval}`);
    }
  }

  private mapSymbolRow(row: any): SymbolMetadata {
    return {
      symbol: row.symbol,
      baseAsset: row.base_asset,
      quoteAsset: row.quote_asset,
      marketType: row.market_type as MarketType,
      status: row.status,
      onboardDate: Number(row.onboard_date),
      contractType: row.contract_type ?? undefined,
      deliveryDate: row.delivery_date ?? undefined,
      filters: {
        tickSize: row.tick_size ?? undefined,
        stepSize: row.step_size ?? undefined,
        minNotional: row.min_notional ?? undefined,
      },
      updatedAt: Number(row.updated_at),
    };
  }

  private async ensureDirectory(): Promise<void> {
    const dir = path.dirname(this.databasePath);
    await fs.promises.mkdir(dir, { recursive: true });
  }

  private async openDatabase(): Promise<void> {
    if (this.db) {
      return;
    }
    this.db = await new Promise<sqlite3.Database>((resolve, reject) => {
      const db = new sqlite3.Database(this.databasePath, (error) => {
        if (error) {
          reject(error);
        } else {
          resolve(db);
        }
      });
    });
  }

  private async configureDatabase(): Promise<void> {
    await this.exec('PRAGMA journal_mode = WAL');
    await this.exec('PRAGMA busy_timeout = 5000');
    await this.exec('PRAGMA synchronous = NORMAL');
  }

  private getDb(): sqlite3.Database {
    if (!this.db) {
      throw new Error('Database connection is not initialized');
    }
    return this.db;
  }

  private async getAppliedMigrations(): Promise<Set<number>> {
    const rows = await this.all<{ id: number }>('SELECT id FROM schema_migrations');
    return new Set(rows.map((row) => row.id));
  }

  private withTransaction<T>(fn: (db: sqlite3.Database) => Promise<T>): Promise<T> {
    const run = async (): Promise<T> => {
      const db = this.getDb();
      await this.exec('BEGIN IMMEDIATE TRANSACTION');
      try {
        const result = await fn(db);
        await this.exec('COMMIT');
        return result;
      } catch (error) {
        await this.exec('ROLLBACK');
        throw error;
      }
    };

    const resultPromise = this.transactionChain.then(run);
    this.transactionChain = resultPromise.then(
      () => undefined,
      () => undefined
    );
    return resultPromise;
  }

  private runSql(db: sqlite3.Database, sql: string, params: unknown[]): Promise<void> {
    return new Promise((resolve, reject) => {
      db.run(sql, params, (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  private run(sql: string, ...params: unknown[]): Promise<void> {
    const db = this.getDb();
    return new Promise((resolve, reject) => {
      db.run(sql, params, (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  private exec(sql: string): Promise<void> {
    const db = this.getDb();
    return new Promise((resolve, reject) => {
      db.exec(sql, (err) => {
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  private all<T = any>(sql: string, params: unknown[] = []): Promise<T[]> {
    const db = this.getDb();
    return new Promise((resolve, reject) => {
      db.all(sql, params, (err, rows) => {
        if (err) {
          reject(err);
        } else {
          resolve(rows as T[]);
        }
      });
    });
  }

  private get<T = any>(sql: string, params: unknown[] = []): Promise<T | undefined> {
    const db = this.getDb();
    return new Promise((resolve, reject) => {
      db.get(sql, params, (err, row) => {
        if (err) {
          reject(err);
        } else {
          resolve((row as T) ?? undefined);
        }
      });
    });
  }
}
