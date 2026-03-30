# 📡 台股中期趨勢偵測系統 (Trend Radar TW)

掃描全部上市櫃 1700+ 檔台股，九維評分偵測中期趨勢。

## 九維評分

| 維度 | 權重 | 說明 |
|------|------|------|
| 相對強度 | 18% | vs 0050.TW 超額報酬 |
| 價格結構 | 15% | 均線多頭排列 |
| 成交量 | 14% | 量能爆發 + 量價配合 |
| 波動率 | 10% | Bollinger Squeeze |
| 族群共振 | 10% | 產業分類 + 概念股群組雙層 |
| 趨勢持續 | 8% | 上漲週比 + 回撤深度 |
| 主題熱度 | 7% | 中文 Google Trends |
| **法人買賣超** | **10%** | 外資/投信連續買超天數 |
| **月營收動能** | **8%** | YoY 加速 + 連續成長 |

## 部署

1. GitHub 建 `trend-radar-tw` repo
2. 上傳所有檔案（用 Git push）
3. 設定 Secrets: `LINE_CHANNEL_ACCESS_TOKEN`, `LINE_USER_ID`, `ANTHROPIC_API_KEY`
4. Actions → Daily TW Scan → Run workflow

排程：週一至週五 台灣 14:30 自動執行
