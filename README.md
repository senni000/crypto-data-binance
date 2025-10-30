# Binance Data Collector

Binance スポット / USDT-M / COIN-M の市場データを収集し、SQLite データベースとバックアップディスクに安全に保存する TypeScript 製コレクタです。1分足のリアルタイム OHLCV は WebSocket 経由で、30分・日足と Top Trader 指標は REST API 経由で取得します。レートリミット・バックアップ・スケジューリングを内包した常駐バッチとして動作します。

## 主な機能

- **シンボル管理**: Exchange Info を毎日取得し、マーケット種別ごとにアクティブ / 非アクティブを自動更新。
- **リアルタイム収集 (1m)**: 市場種別ごとに WebSocket 接続をプールし、最大 300 シンボル単位でストリームを分割、切断時は指数バックオフで再接続。
- **履歴収集 (30m / 1d)**: REST API から未保存区間のみを補完し、タイムスタンプ + シンボル複合キーで重複を排除。
- **Top Trader 指標 (5m)**: USDT-M の Long/Short ポジション比率・アカウント比率を5分毎に取得し履歴化。
- **レートリミッタ**: トークンバケット + 優先度キューでエンドポイント別ウェイトを制御。429 受信時は指数バックオフ + ジッタで自動再試行。
- **SQLite 永続化**: シンボル、3種類の OHLCV、Top Trader の各テーブルにトランザクションバッチで書き込み。シンボルはマーケット種別との複合主キーを採用。
- **バックアップ**: 指定ディレクトリに日次スナップショットを生成。直近 7 日分の日次バックアップと最新 1 件の週次バックアップを保持し、その他は削除。バックアップ実行時に 7 日より古い OHLCV／Top Trader レコードも自動削除。

## プロジェクト構成

```
src/
  index.ts                    BINANCE_PROCESS_ROLE に応じたエントリーディスパッチ
  processes/
    ingest.ts                 WebSocket/REST インジェスト専用プロセス
    aggregate.ts              CVD 集計ワーカー (trade_data → cvd_data)
    alert.ts                  アラートディスパッチャー (cvd_data → Discord)
  services/
    data-collector.ts         REST データ収集スケジューラ
    trade-data-collector.ts   WebSocket インジェストとバッファ管理
    cvd-aggregation-worker.ts CVD 集計キュー処理
    alert-queue-processor.ts  アラートキュー監視と再送制御
    symbol-manager.ts         Binance シンボル管理
    binance-rest-client.ts    REST クライアント
    rate-limiter.ts           トークンバケット型レートリミッタ
    database.ts               SQLite マネージャ (マイグレーション内蔵)
    database-backup-scheduler.ts  バックアップ＆保持ポリシー
  types/                      ドメイン型定義
  utils/                      設定ロード・ロガーなど
```

## 主要テーブル

| テーブル | 用途 | 主キー |
|---|---|---|
| `symbols` | シンボルメタデータ (マーケット種別・状態) | `(symbol, market_type)` |
| `ohlcv_1m` | 1分足 (WebSocket) | `(symbol, open_time)` |
| `ohlcv_30m` | 30分足 (REST) | `(symbol, open_time)` |
| `ohlcv_1d` | 日足 (REST) | `(symbol, open_time)` |
| `top_trader_positions` | Top Trader ポジション比率 | `(symbol, timestamp)` |
| `top_trader_accounts` | Top Trader アカウント比率 | `(symbol, timestamp)` |

## セットアップ

1. 依存関係のインストール

   ```bash
   npm install
   ```

2. 環境変数の準備

   ```bash
   cp .env.example .env
   ```

   主な設定項目 (括弧内は既定値):

   - `DATABASE_PATH`: メイン SQLite ファイルのパス (`~/workspace/crypto-data/data/binance.db`)。
   - `DATABASE_BACKUP_ENABLED`: バックアップスケジューラを有効化するか (`true` / 無効化する場合は `false`)。
   - `DATABASE_BACKUP_SINGLE_FILE`: `true` にするとタイムスタンプ付きスナップショットではなく単一ファイルに上書き保存。
   - `DATABASE_BACKUP_PATH`: バックアップディレクトリ (`/Volumes/buffalohd/crypto-data/backups/binance`)。
  - `DATABASE_BACKUP_INTERVAL_MS`: バックアップの実行間隔ミリ秒 (`86400000`)。
  - `BINANCE_REST_URL` / `BINANCE_USDM_REST_URL` / `BINANCE_COINM_REST_URL`: REST API ベース URL。
  - `RATE_LIMIT_BUFFER`: レートリミットキャパシティに掛ける安全係数 (`0.1`)。
  - `REST_REQUEST_TIMEOUT_MS`: REST リクエストのタイムアウト (`10000`)。
  - `SYMBOL_UPDATE_HOUR_UTC`: シンボル同期の実行時刻 (UTC, `1`)。
  - `LOG_LEVEL`: `error` / `warn` / `info` / `debug` (`info`)。
  - `CVD_AGGREGATION_BATCH_SIZE`: CVD 集計で 1 バッチに読み込むトレード件数 (`500`)。
  - `CVD_AGGREGATION_POLL_INTERVAL_MS`: 集計ワーカーのポーリング間隔ミリ秒 (`2000`)。
  - `CVD_ALERT_SUPPRESSION_MINUTES`: 同一シンボルのアラート抑止ウィンドウ (分, `30`)。
  - `ALERT_QUEUE_POLL_INTERVAL_MS`: アラートキュー監視のポーリング間隔ミリ秒 (`2000`)。
  - `ALERT_QUEUE_BATCH_SIZE`: アラート処理時に取得する最大件数 (`20`)。
  - `ALERT_QUEUE_MAX_ATTEMPTS`: 送信失敗時の再試行上限 (`5`)。

3. ビルド & 実行

   ```bash
   pnpm run build                     # TypeScript を dist/ にコンパイル
   pnpm run start:ingest              # WebSocket/REST インジェスト
   pnpm run start:aggregate           # CVD 集計ワーカー
   pnpm run start:alert               # アラート送信 (ENABLE_CVD_ALERTS=true の場合)

   # ts-node 開発モード
   pnpm run dev:ingest
   pnpm run dev:aggregate
   pnpm run dev:alert
   ```

   ※ `BINANCE_PROCESS_ROLE` を直接指定する場合は `BINANCE_PROCESS_ROLE=aggregate pnpm start`
   のように環境変数を渡してください。

4. PM2 常駐

   `ecosystem.config.js` には `binance-ingest` / `binance-aggregate` / `binance-alert` の 3 プロセスが定義されています。
   まとめて起動・停止するには以下を利用してください。

   ```bash
   pnpm run build
   pnpm run start:pm2
   pnpm run stop:pm2
   ```

4. テスト

   ```bash
   npm test
   ```

   テストは実際の Binance API レスポンス (サンプル JSON) をフィクスチャとして使用し、DOM 外でも現実的なデータ形式を検証します。

## 動作概要

1. 起動時に `.env` を読み込み、`RateLimiter` にエンドポイント別のキャパシティを登録。
2. `SymbolManager.updateSymbols()` で SPOT / USDT-M / COIN-M のシンボルを取得し、SQLite にアップサート。
3. `AggTradeCollector` が主要ペアを毎時 REST API で取得し、ティックデータを SQLite に蓄積 (初回のみ過去 12 時間分をバックフィル)。
4. Top Trader 指標を 5 分ごとに取得し、リトライとディレイを挟みつつポジション/アカウント比率テーブルを更新。
5. バックアップスケジューラが日次で SQLite をコピーし、保持ポリシーに沿ってバックアップを整理。併せて 7 日より古い OHLCV / Top Trader データをプライマリ DB から間引き。`DATABASE_BACKUP_ENABLED=false` の場合は本処理をスキップ。

## ライセンス

MIT License
