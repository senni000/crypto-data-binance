# Crypto Data Alert System

暗号通貨データ収集・アラートシステム - Deribit APIからBTCデータを収集し、Discord Webhookでアラート通知を送信するシステムです。

## 機能

- Deribit WebSocket APIを使用したBTC約定データのリアルタイム収集
- Deribit REST APIを使用したBTCオプションデータの定期収集
- SQLiteデータベースでのデータ永続化
- CVD（Cumulative Volume Delta）のZ-score監視
- C-P Δ25移動平均線の変化監視
- Discord Webhookによるアラート通知

## 実装機能とデータ保存先

### データ収集
- **トレード（WebSocket）**  
  - サブスクライブ対象: `trades.BTC-PERPETUAL.100ms`, `trades.BTC-PERPETUAL-USDC.100ms`, `trades.BTC-USD.100ms`  
  - 処理: `src/services/data-collector.ts` が `DeribitWebSocketClient` から受信した約定を検証・バッファし、`DatabaseManager.saveTradeData` で永続化  
  - 利用: CVD 計算、Discord アラート

- **オプション（REST）**  
  - エンドポイント: `/public/get_instruments`, `/public/get_order_book`  
  - 処理: `DeribitRestClient` が `.env` の `OPTION_DATA_INTERVAL` 毎にバッチ取得し、`saveOptionData` で保存  
  - 利用: C-P Δ25 アラート計算

### 指標・アラート
- **CVD (Cumulative Volume Delta)**  
  - 計算: `CVDCalculator.calculateBTCPerpetualCVD` が対象シンボル全体を合算  
  - 永続化: `AlertManager.checkCVDAlert` 内で `cvd_data` テーブルに保存（値・Zスコア）  
  - アラート: Zスコアが閾値 (`CVD_ZSCORE_THRESHOLD`) 超過時に Discord へ送信、`alert_history` へ記録

- **CVDグラフ配信**  
  - スケジューラ: `CVDReportScheduler` が毎時起動 (`src/services/cvd-report-scheduler.ts`)  
  - データ: 直近60分の `cvd_data` を取得 (`getCVDDataSince`)  
  - 出力: QuickChart API のグラフURLを生成し、Discordへ埋め込み送信

- **C-P Δ25 アラート**  
  - 処理: `AlertManager.checkCPDelta25Alert` がデルタ ±0.25 の銘柄を選定し、移動平均変化を監視  
  - 条件達成で Discord 通知、`alert_history` に履歴を保存

### 可用性・ログ
- **ヘルスモニタ** (`DataHealthMonitor`)  
  - 監視対象: システムスリープ検知、WebSocket停滞、オプション遅延、RESTエラー急増  
  - 対応: 自動リカバリー（再接続・収集再開）と警告ログ出力

- **データバックアップ** (`DatabaseBackupScheduler`)  
  - `.env` の `DATABASE_BACKUP_ENABLED=true` 時に有効化  
  - 指定した間隔で `DATABASE_PATH` から `DATABASE_BACKUP_PATH` へコピー（既存ファイルを上書き）  
  - 失敗・スキップ時はログに理由を出力

- **ログ**  
  - `src/utils/setup-logging.ts` が `console.*` に ISO8601 タイムスタンプを付加  
  - PM2 運用時の出力: `~/.pm2/logs/crypto-data-alert-out-0.log`, `...-error-0.log`

### SQLite テーブル構成（`.env` の `DATABASE_PATH` に出力）
| テーブル       | 役割                      | 主なカラム |
|---------------|---------------------------|------------|
| `trade_data`  | WebSocket 約定履歴        | `symbol`, `timestamp`, `price`, `amount`, `direction`, `trade_id`, `created_at` |
| `option_data` | REST 取得オプション情報    | `symbol`, `timestamp`, `underlying_price`, `mark_price`, `delta` など |
| `cvd_data`    | CVD 値と Zスコアの履歴     | `timestamp`, `cvd_value`, `z_score`, `created_at` |
| `alert_history` | 送信済みアラートの記録   | `alert_type`, `timestamp`, `value`, `threshold`, `message`, `created_at` |

## セットアップ

### 1. 依存関係のインストール

```bash
npm install
```

### 2. 環境変数の設定

`.env.example`をコピーして`.env`ファイルを作成し、必要な値を設定してください：

```bash
cp .env.example .env
```

### 3. 必要な設定項目

- `DISCORD_WEBHOOK_URL`: Discord WebhookのURL
- `DATABASE_PATH`: SQLiteデータベースファイルのパス
- `LOG_LEVEL`: ログ出力レベル（`error` / `warn` / `info` / `debug`）
- `DATABASE_BACKUP_ENABLED`: 定期バックアップを有効にする場合は `true`  
- `DATABASE_BACKUP_PATH`: バックアップ先（例: `/Volumes/buffalohd/crypto_data.db`）
- `DATABASE_BACKUP_INTERVAL`: バックアップ間隔 (ms) ※ デフォルト 3600000 (1時間)
- その他の設定は`.env.example`を参照

### 4. ビルドと実行

```bash
# TypeScriptをコンパイル
npm run build

# アプリケーションを実行
npm start

# 開発モード（TypeScriptを直接実行）
npm run dev

# PM2で常駐起動
npm run start:pm2
```

### 5. テスト実行

```bash
npm test
```

### 6. パフォーマンス計測

擬似データを用いたSQLite書き込み性能を計測するハーネスを用意しています。

```bash
npm run perf
```

デフォルトでは一時ディレクトリにデータベースを作成し、完了後に削除します。`PERF_KEEP_DB=true` でファイルを保持、`PERF_TRADE_BATCH` や `PERF_OPTION_BATCH` で投入件数を調整できます。

### 7. 短時間の動作確認（SQLite保存 + Discord通知）

実際のDeribit APIに接続してデータ収集・SQLite保存・アラートパイプラインを検証するスクリプトを用意しています。

```bash
# 30秒だけ収集し、Discord送信はドライラン（payloadをログ出力）のまま
COLLECT_DURATION_MS=30000 ENABLE_ALERTS=false npm run collect:sample

# Discord通知まで行いたい場合（本番Webhookを設定）
DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/..." \
COLLECT_DURATION_MS=60000 ENABLE_ALERTS=true npm run collect:sample
```

※ Discord通知を有効にする場合は、必ず検証用チャンネルまたはWebhookを事前に用意してください。

## 運用メモ

- 常駐運用には [PM2](https://pm2.keymetrics.io/) の利用を想定しており、`ecosystem.config.js` を同梱しています。
- `npm run start:pm2` / `npm run stop:pm2` でプロセスの起動・停止を行えます。
- ログレベルは `.env` の `LOG_LEVEL` で制御でき、`debug` に設定すると詳細ログを出力します。

## プロジェクト構造

```
src/
├── models/     # データモデル定義
├── services/   # ビジネスロジック
├── utils/      # ユーティリティ関数
└── types/      # TypeScript型定義
```

## 要件

- Node.js 18以上
- SQLite3
- 十分なディスク容量（データベース保存用）

## ライセンス

MIT
