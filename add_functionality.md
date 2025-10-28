

目的
CoinMarketCap 上位100（BTC除外） の銘柄について
→ 対応する Binance Spot と Perp（USDT無期限） の約定データ (aggTrade) を WebSocket で取得・保管する。

大口の抜け（出来高変化やCVD反転）と価格下落の相関を検証するためのデータ基盤。

🧩 運用設計
項目	方針
データ対象	CoinMarketCap Top100（BTC除外）
ペア種別	各銘柄の USDT Spot ＋ USDT Perp（無期限）
データ取得	WebSocket (@aggTrade)
補完処理	REST /aggTrades による 12時間ごとの欠落チェック＋補完
保存形式	SQLite（WALモードのみ）
リスト更新	固定（coinmarketcap_top100.csvからロード）
除外ルール	- BTC除外
- ステーブル（USDT/USDC/FDUSD/TUSD/DAI など）は手動除外候補
保存周期	シンボル単位SQLite構成（Spot+Perp）＋12h欠落補完
監視項目	再接続回数、書き込み遅延、欠落検出件数、補完成功件数
他のデータと同じようにバックアップも外付けHDに行う。