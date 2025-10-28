# Binance Data Collector

Binance スポット / USDT-M / COIN-M の市場データを収集し、SQLite データベースとバックアップディスクに安全に保存する TypeScript 製コレクタです。1分足のリアルタイム OHLCV は WebSocket 経由で、30分・日足と Top Trader 指標は REST API 経由で取得します。レートリミット・バックアップ・スケジューリングを内包した常駐バッチとして動作します。

## 主な機能

- **シンボル管理**: Exchange Info を毎日取得し、マーケット種別ごとにアクティブ / 非アクティブを自動更新。
- **リアルタイム収集 (1m)**: 市場種別ごとに WebSocket 接続をプールし、最大 300 シンボル単位でストリームを分割、切断時は指数バックオフで再接続。
- **履歴収集 (30m / 1d)**: REST API から未保存区間のみを補完し、タイムスタンプ + シンボル複合キーで重複を排除。
- **Top Trader 指標 (5m)**: USDT-M の Long/Short ポジション比率・アカウント比率を5分毎に取得し履歴化。
- **レートリミッタ**: トークンバケット + 優先度キューでエンドポイント別ウェイトを制御。429 受信時は指数バックオフ + ジッタで自動再試行。
- **SQLite 永続化**: シンボル、3種類の OHLCV、Top Trader の各テーブルにトランザクションバッチで書き込み。シンボルはマーケット種別との複合主キーを採用。
- **バックアップ**: 指定ディレクトリに日次スナップショットを生成。直近 30 日のデイリーバックアップと 12 週間のウィークリーバックアップを自動保持し、古いファイルは削除。

## プロジェクト構成

```
src/
  index.ts                    エントリーポイント
  services/
    data-collector.ts         WebSocket/REST 連携とスケジューラ
    symbol-manager.ts         Binance シンボル管理
    binance-rest-client.ts    OHLCV・Top Trader REST クライアント
    binance-websocket-manager.ts  WebSocket 接続管理
    rate-limiter.ts           トークンバケット型レートリミッタ
    database.ts               SQLite マネージャ (マイグレーション内蔵)
    database-backup-scheduler.ts  バックアップ＆保持ポリシー
  types/                      ドメイン型定義
  utils/                      設定ロード・ロガーなど
.kiro/specs/                  仕様・タスクドキュメント
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

   主な設定項目:

   - `DATABASE_PATH`: メイン SQLite ファイル。
   - `DATABASE_BACKUP_PATH`: バックアップディレクトリ (日次バックアップを生成)。
   - `BINANCE_REST_URL`, `BINANCE_USDM_REST_URL`, `BINANCE_COINM_REST_URL`: REST エンドポイント。
   - `BINANCE_WS_URL`, `BINANCE_USDM_WS_URL`, `BINANCE_COINM_WS_URL`: WebSocket エンドポイント。
   - `RATE_LIMIT_BUFFER`: レートリミットの安全マージン (0.1 = 10% 減)。
   - `WS_MAX_SYMBOLS_PER_STREAM`: WebSocket 1 接続あたりの登録上限 (規定値 300)。

3. ビルド & 実行

   ```bash
   npm run build   # TypeScript を dist/ にコンパイル
   npm start       # dist/index.js を実行
   # 開発モード
   npm run dev     # ts-node でホット実行
   ```

4. テスト

   ```bash
   npm test
   ```

   テストは実際の Binance API レスポンス (サンプル JSON) をフィクスチャとして使用し、DOM 外でも現実的なデータ形式を検証します。

## 動作概要

1. 起動時に `.env` を読み込み、`RateLimiter` にエンドポイント別のキャパシティを登録。
2. `SymbolManager.updateSymbols()` で SPOT / USDT-M / COIN-M のシンボルを取得し、SQLite にアップサート。
3. `BinanceWebSocketManager` が 1 分足ストリームを購読し、バッファを介して 5 秒ごとに `ohlcv_1m` へバルク挿入。
4. REST スケジューラが 30 分足・日足を定期取得し、未取得期間だけを追加入力。
5. Top Trader 指標を 5 分ごとに取得し、ポジションとアカウント比率テーブルを更新。
6. バックアップスケジューラが日次で SQLite をコピーし、指定保有期間外のファイルを削除。併せて古い OHLCV/Top Trader データをプライマリ DB から間引き。

## ライセンス

MIT License
