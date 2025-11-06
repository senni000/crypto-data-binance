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
  TradeData,
  CVDData,
  AlertHistory,
  TradeDataRow,
  AlertQueueRecord,
  LiquidationEvent,
} from '../types';
import { IDatabaseManager, ProcessingState } from './interfaces';
import { CvdAlertPayload } from '@crypto-data/cvd-core';

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
  {
    id: 3,
    name: 'create_trade_and_alert_tables',
    statements: [
      `CREATE TABLE IF NOT EXISTS trade_data (
        symbol TEXT NOT NULL,
        market_type TEXT NOT NULL,
        trade_id TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        price REAL NOT NULL,
        amount REAL NOT NULL,
        direction TEXT NOT NULL,
        stream_type TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (symbol, market_type, trade_id)
      )`,
      `CREATE TABLE IF NOT EXISTS cvd_data (
        symbol TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        cvd_value REAL NOT NULL,
        z_score REAL NOT NULL,
        delta_value REAL NOT NULL DEFAULT 0,
        delta_z_score REAL NOT NULL DEFAULT 0,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (symbol, timestamp)
      )`,
      `CREATE TABLE IF NOT EXISTS alert_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        alert_type TEXT NOT NULL,
        symbol TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        value REAL NOT NULL,
        threshold REAL NOT NULL,
        message TEXT NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`,
      `CREATE INDEX IF NOT EXISTS idx_trade_data_timestamp ON trade_data(timestamp)`,
      `CREATE INDEX IF NOT EXISTS idx_trade_data_symbol_market ON trade_data(symbol, market_type)`,
      `CREATE INDEX IF NOT EXISTS idx_cvd_data_symbol_timestamp ON cvd_data(symbol, timestamp)`,
      `CREATE INDEX IF NOT EXISTS idx_alert_history_lookup ON alert_history(alert_type, symbol, timestamp)`
    ],
  },
  {
    id: 4,
    name: 'create_processing_state_and_alert_queue',
    statements: [
      `CREATE TABLE IF NOT EXISTS processing_state (
        process_name TEXT NOT NULL,
        key TEXT NOT NULL,
        last_row_id INTEGER NOT NULL,
        last_timestamp INTEGER NOT NULL,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (process_name, key)
      )`,
      `CREATE TABLE IF NOT EXISTS alert_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        alert_type TEXT NOT NULL,
        symbol TEXT NOT NULL,
        timestamp INTEGER NOT NULL,
        trigger_source TEXT NOT NULL,
        trigger_z_score REAL NOT NULL,
        z_score REAL NOT NULL,
        delta REAL NOT NULL,
        delta_z_score REAL NOT NULL,
        threshold REAL NOT NULL,
        cumulative_value REAL NOT NULL,
        payload_json TEXT NOT NULL,
        attempt_count INTEGER NOT NULL DEFAULT 0,
        last_error TEXT,
        processed_at DATETIME,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`,
      `CREATE INDEX IF NOT EXISTS idx_alert_queue_pending ON alert_queue(processed_at, timestamp)`,
      `CREATE INDEX IF NOT EXISTS idx_alert_queue_symbol ON alert_queue(symbol, processed_at)`,
      `CREATE INDEX IF NOT EXISTS idx_alert_queue_type ON alert_queue(alert_type, processed_at)`
    ],
  },
  {
    id: 5,
    name: 'create_liquidation_events_table',
    statements: [
      `CREATE TABLE IF NOT EXISTS liquidation_events (
        event_id TEXT PRIMARY KEY,
        symbol TEXT NOT NULL,
        market_type TEXT NOT NULL,
        side TEXT NOT NULL,
        price REAL NOT NULL,
        average_price REAL,
        last_filled_price REAL,
        original_quantity REAL NOT NULL,
        filled_quantity REAL NOT NULL,
        last_filled_quantity REAL,
        event_time INTEGER NOT NULL,
        trade_time INTEGER,
        order_type TEXT,
        time_in_force TEXT,
        status TEXT,
        order_id TEXT,
        is_maker INTEGER NOT NULL,
        reduce_only INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
      )`,
      `CREATE INDEX IF NOT EXISTS idx_liquidation_events_symbol_time ON liquidation_events(symbol, market_type, event_time)`
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
      await this.ensureCvdDeltaColumns();
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

  async saveTradeData(data: TradeData[]): Promise<void> {
    if (data.length === 0) {
      return;
    }

    const sql = `
      INSERT OR IGNORE INTO trade_data (
        symbol, market_type, trade_id, timestamp, price, amount, direction, stream_type
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `;

    await this.withTransaction(async (db) => {
      for (const trade of data) {
        await this.runSql(db, sql, [
          trade.symbol,
          trade.marketType,
          trade.tradeId,
          trade.timestamp,
          trade.price,
          trade.amount,
          trade.direction,
          trade.streamType,
        ]);
      }
    });
  }

  async saveLiquidationEvents(events: LiquidationEvent[]): Promise<void> {
    if (events.length === 0) {
      return;
    }

    const sql = `
      INSERT OR IGNORE INTO liquidation_events (
        event_id,
        symbol,
        market_type,
        side,
        price,
        average_price,
        last_filled_price,
        original_quantity,
        filled_quantity,
        last_filled_quantity,
        event_time,
        trade_time,
        order_type,
        time_in_force,
        status,
        order_id,
        is_maker,
        reduce_only,
        created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `;

    await this.withTransaction(async (db) => {
      for (const event of events) {
        await this.runSql(db, sql, [
          event.eventId,
          event.symbol,
          event.marketType,
          event.side,
          event.price,
          event.averagePrice ?? null,
          event.lastFilledPrice ?? null,
          event.originalQuantity,
          event.filledQuantity,
          event.lastFilledQuantity ?? null,
          event.eventTime,
          event.tradeTime ?? null,
          event.orderType ?? null,
          event.timeInForce ?? null,
          event.status ?? null,
          event.orderId ?? null,
          event.isMaker ? 1 : 0,
          event.reduceOnly === undefined ? null : event.reduceOnly ? 1 : 0,
          new Date(event.createdAt).toISOString(),
        ]);
      }
    });
  }

  async saveCVDData(data: CVDData): Promise<void> {
    await this.run(
      `INSERT OR REPLACE INTO cvd_data (symbol, timestamp, cvd_value, z_score, delta_value, delta_z_score)
       VALUES (?, ?, ?, ?, ?, ?)`,
      data.symbol,
      data.timestamp,
      data.cvdValue,
      data.zScore,
      data.delta ?? 0,
      data.deltaZScore ?? 0
    );
  }

  async getCVDDataSince(symbol: string, since: number): Promise<CVDData[]> {
    const rows = await this.all<{
      symbol: string;
      timestamp: number;
      cvd_value: number;
      z_score: number;
      delta_value: number;
      delta_z_score: number;
    }>(
      `SELECT symbol, timestamp, cvd_value, z_score, delta_value, delta_z_score
       FROM cvd_data
       WHERE symbol = ? AND timestamp >= ?
       ORDER BY timestamp ASC`,
      [symbol, since]
    );
    return rows.map((row) => ({
      symbol: row.symbol,
      timestamp: Number(row.timestamp),
      cvdValue: Number(row.cvd_value),
      zScore: Number(row.z_score),
      delta: Number(row.delta_value ?? 0),
      deltaZScore: Number(row.delta_z_score ?? 0),
    }));
  }

  async saveAlertHistory(alert: AlertHistory): Promise<void> {
    await this.run(
      `INSERT INTO alert_history (
        alert_type, symbol, timestamp, value, threshold, message
      ) VALUES (?, ?, ?, ?, ?, ?)`,
      alert.alertType,
      alert.symbol,
      alert.timestamp,
      alert.value,
      alert.threshold,
      alert.message
    );
  }

  async getRecentAlerts(alertType: string, symbol: string, minutes = 30): Promise<AlertHistory[]> {
    const cutoff = Date.now() - minutes * 60 * 1000;
    const rows = await this.all<{
      id: number;
      alert_type: string;
      symbol: string;
      timestamp: number;
      value: number;
      threshold: number;
      message: string;
      created_at: string;
    }>(
      `SELECT id, alert_type, symbol, timestamp, value, threshold, message, created_at
       FROM alert_history
       WHERE alert_type = ? AND symbol = ? AND timestamp >= ?
       ORDER BY timestamp DESC`,
      [alertType, symbol, cutoff]
    );

    return rows.map((row) => ({
      id: row.id,
      alertType: row.alert_type,
      symbol: row.symbol,
      timestamp: Number(row.timestamp),
      value: Number(row.value),
      threshold: Number(row.threshold),
      message: row.message,
      createdAt: row.created_at,
    }));
  }

  async getTradeDataSinceRowId(
    filters: Array<{ symbol: string; marketType: MarketType; streamType: TradeData['streamType'] }>,
    lastRowId: number,
    limit: number
  ): Promise<TradeDataRow[]> {
    if (filters.length === 0) {
      return [];
    }

    const conditions = filters
      .map(() => '(symbol = ? AND market_type = ? AND stream_type = ?)')
      .join(' OR ');
    const params: Array<string | number> = [lastRowId];
    for (const filter of filters) {
      params.push(filter.symbol.toUpperCase(), filter.marketType, filter.streamType);
    }
    params.push(Math.max(1, Math.floor(limit)));

    const rows = await this.all<{
      rowId: number;
      symbol: string;
      marketType: string;
      tradeId: string;
      timestamp: number;
      price: number;
      amount: number;
      direction: string;
      streamType: string;
    }>(
      `SELECT rowid as rowId, symbol, market_type as marketType, trade_id as tradeId, timestamp, price, amount, direction, stream_type as streamType
       FROM trade_data
       WHERE rowid > ? AND (${conditions})
       ORDER BY rowid ASC
       LIMIT ?`,
      params
    );

    return rows.map((row) => ({
      rowId: Number(row.rowId),
      symbol: row.symbol,
      marketType: row.marketType as MarketType,
      tradeId: row.tradeId,
      timestamp: Number(row.timestamp),
      price: Number(row.price),
      amount: Number(row.amount),
      direction: row.direction as TradeData['direction'],
      streamType: row.streamType as TradeData['streamType'],
    }));
  }

  async getProcessingState(processName: string, key: string): Promise<ProcessingState | null> {
    const row = await this.get<{ last_row_id: number; last_timestamp: number }>(
      `SELECT last_row_id, last_timestamp
       FROM processing_state
       WHERE process_name = ? AND key = ?`,
      [processName, key]
    );

    if (!row) {
      return null;
    }

    return {
      lastRowId: Number(row.last_row_id ?? 0),
      lastTimestamp: Number(row.last_timestamp ?? 0),
    };
  }

  async saveProcessingState(processName: string, key: string, state: ProcessingState): Promise<void> {
    await this.run(
      `INSERT INTO processing_state (process_name, key, last_row_id, last_timestamp, updated_at)
       VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
       ON CONFLICT(process_name, key)
       DO UPDATE SET last_row_id = excluded.last_row_id,
                     last_timestamp = excluded.last_timestamp,
                     updated_at = CURRENT_TIMESTAMP`,
      processName,
      key,
      state.lastRowId,
      state.lastTimestamp
    );
  }

  async enqueueAlert(alertType: string, payload: CvdAlertPayload): Promise<number> {
    return new Promise<number>((resolve, reject) => {
      const db = this.getDb();
      db.run(
        `INSERT INTO alert_queue (
          alert_type,
          symbol,
          timestamp,
          trigger_source,
          trigger_z_score,
          z_score,
          delta,
          delta_z_score,
          threshold,
          cumulative_value,
          payload_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          alertType,
          payload.symbol,
          payload.timestamp,
          payload.triggerSource,
          payload.triggerZScore,
          payload.zScore,
          payload.delta,
          payload.deltaZScore,
          payload.threshold,
          payload.cumulativeValue,
          JSON.stringify(payload),
        ],
        function (err) {
          if (err) {
            reject(err);
          } else {
            resolve(Number(this.lastID));
          }
        }
      );
    });
  }

  async getPendingAlerts(limit: number): Promise<AlertQueueRecord[]> {
    const rows = await this.all<{
      id: number;
      alert_type: string;
      payload_json: string;
      attempt_count: number;
      last_error: string | null;
      processed_at: string | null;
      created_at: string;
    }>(
      `SELECT id, alert_type, payload_json, attempt_count, last_error, processed_at, created_at
       FROM alert_queue
       WHERE processed_at IS NULL
       ORDER BY timestamp ASC, id ASC
       LIMIT ?`,
      [Math.max(1, Math.floor(limit))]
    );

    return rows.map((row) => {
      const record: AlertQueueRecord = {
        id: row.id,
        alertType: row.alert_type,
        payload: JSON.parse(row.payload_json) as CvdAlertPayload,
        attemptCount: Number(row.attempt_count ?? 0),
        lastError: row.last_error,
        processedAt: row.processed_at ? new Date(row.processed_at).getTime() : null,
      };

      if (row.created_at) {
        record.enqueuedAt = new Date(row.created_at).getTime();
      }

      return record;
    });
  }

  async markAlertAttempt(id: number): Promise<void> {
    await this.run(
      `UPDATE alert_queue
       SET attempt_count = attempt_count + 1,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      id
    );
  }

  async markAlertProcessed(id: number, options: { clearError?: boolean } = {}): Promise<void> {
    const clearError = options.clearError ?? true;
    await this.run(
      `UPDATE alert_queue
       SET processed_at = CURRENT_TIMESTAMP,
           last_error = CASE WHEN ? THEN NULL ELSE last_error END,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      clearError ? 1 : 0,
      id
    );
  }

  async markAlertFailure(id: number, error: string): Promise<void> {
    await this.run(
      `UPDATE alert_queue
       SET last_error = ?,
           updated_at = CURRENT_TIMESTAMP
       WHERE id = ?`,
      this.truncateErrorMessage(error),
      id
    );
  }

  async hasRecentAlertOrPending(alertType: string, symbol: string, sinceTimestamp: number): Promise<boolean> {
    const pending = await this.get<{ exists: number }>(
      `SELECT 1 as exists
       FROM alert_queue
       WHERE alert_type = ?
         AND symbol = ?
         AND (
           processed_at IS NULL
           OR timestamp >= ?
         )
       LIMIT 1`,
      [alertType, symbol, sinceTimestamp]
    );

    if (pending) {
      return true;
    }

    const history = await this.get<{ exists: number }>(
      `SELECT 1 as exists
       FROM alert_history
       WHERE alert_type = ? AND symbol = ? AND timestamp >= ?
       LIMIT 1`,
      [alertType, symbol, sinceTimestamp]
    );

    return Boolean(history);
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

  private async ensureCvdDeltaColumns(): Promise<void> {
    const hasDeltaValue = await this.columnExists('cvd_data', 'delta_value');
    if (!hasDeltaValue) {
      await this.run('ALTER TABLE cvd_data ADD COLUMN delta_value REAL NOT NULL DEFAULT 0');
    }

    const hasDeltaZScore = await this.columnExists('cvd_data', 'delta_z_score');
    if (!hasDeltaZScore) {
      await this.run('ALTER TABLE cvd_data ADD COLUMN delta_z_score REAL NOT NULL DEFAULT 0');
    }
  }

  private truncateErrorMessage(message: string, maxLength = 512): string {
    const normalized = message ?? '';
    if (normalized.length <= maxLength) {
      return normalized;
    }
    return `${normalized.slice(0, Math.max(0, maxLength - 3))}...`;
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

  private async columnExists(table: string, column: string): Promise<boolean> {
    const rows = await this.all<{ name: string }>(`PRAGMA table_info(${table})`);
    return rows.some((row) => row.name === column);
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
