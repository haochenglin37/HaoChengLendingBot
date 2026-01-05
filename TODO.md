TODO
====

1. 成交資料收集
   - 建立新的 SQLite 資料表，記錄 USD funding 的 `timestamp/rate/amount/duration`。
   - 每 5 秒或固定頻率打 Bitfinex funding trades API，僅存最近 4 小時資料（或定期清理）。

2. Bucket 策略分析
   - 依天數分成四個 bucket（示例：2d、3-7d、8-30d、30d+），統計各自 4 小時內的利率分布。
   - 計算各 bucket 的 `P85` 作為掛單利率，若 bucket 無資料需回退或把本金平均到其他 bucket。
   - 長天期 bucket 的 percentile 可調高（例如 P90 或 P95）以補償等待風險；提供 config 參數或動態調整邏輯（依成交量/等待時間調整 P 值）。

3. 掛單與天數配置
   - 將可貸本金拆成四等份或按 bucket 成交量加權，再依 `minloansize` 切片下單。
   - 天數與 bucket 對應，例如最長 bucket 固定 120 天，最短 30 天，可依利率接近 `FRR_high` 的程度微調。
   - 設定最大等待時間：超時則降低利率或縮短天數，並記錄乘數／天期狀態。

4. Config 與方法整合
   - 在 `default.cfg` 增加新的 `Daily_min.method = bucket_percentile_frr` 及相關設定（bucket 邊界、percentile、權重、等待時間）。
   - `MarketAnalysis` 新增 method：讀 4 小時成交資料、計算 FRR + σ、回傳 `FRR_high`/bucket 建議。
   - `Lending` 新增策略接口，允許跳過 `construct_orders`，直接使用 bucket 結果生成訂單。

5. 測試 & 驗證
   - 建立單元測試，mock 成交資料確認 bucket 計算與掛單分配。
   - 使用 `--dryrun` 實測 log，確保四個 bucket 都能正確產生訂單並在超時時降檔。
