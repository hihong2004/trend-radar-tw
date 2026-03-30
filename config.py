"""
config.py — 台股趨勢雷達全域設定
九維評分（含法人買賣超 + 月營收）、閾值、API keys
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── API Keys ──────────────────────────────────────────────
LINE_CHANNEL_ACCESS_TOKEN = os.getenv("LINE_CHANNEL_ACCESS_TOKEN", "")
LINE_USER_ID = os.getenv("LINE_USER_ID", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

# ── LINE API ──────────────────────────────────────────────
LINE_PUSH_URL = "https://api.line.me/v2/bot/message/push"

# ── 基準標的 ──────────────────────────────────────────────
BENCHMARK = "0050.TW"
MARKET_INDEX = "^TWII"

# ── yfinance 台股代碼後綴 ─────────────────────────────────
TWSE_SUFFIX = ".TW"   # 上市
TPEX_SUFFIX = ".TWO"  # 上櫃

# ── 九維評分權重 ──────────────────────────────────────────
SCORING_WEIGHTS = {
    "relative_strength": 0.18,
    "price_structure": 0.15,
    "volume_analysis": 0.14,
    "volatility": 0.10,
    "sector_momentum": 0.10,
    "trend_consistency": 0.08,
    "theme_momentum": 0.07,
    "institutional_flow": 0.10,   # 新：法人買賣超
    "revenue_momentum": 0.08,     # 新：月營收動能
}

# ── 等級劃分 ──────────────────────────────────────────────
RATING_THRESHOLDS = {
    5: 85,
    4: 70,
    3: 55,
    2: 40,
    1: 0,
}

# ── Stage 判定參數 ────────────────────────────────────────
STAGE_PARAMS = {
    "awakening_threshold": 55,
    "acceleration_threshold": 70,
    "acceleration_hold_days": 10,
    "sector_confirm_threshold": 50,
    "decay_drop_points": 15,
    "rs_decay_percentile": 50,
}

# ── 數據參數 ──────────────────────────────────────────────
DATA_HISTORY_YEARS = 2
DATA_BATCH_SIZE = 50
DATA_BATCH_DELAY = 2
INCREMENTAL_DAYS = 10
MIN_DAILY_VOLUME = 500    # 最低日均成交量（張），過濾冷門股

# ── 證交所 / 櫃買中心 API ─────────────────────────────────
TWSE_STOCK_LIST_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TPEX_STOCK_LIST_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
TWSE_INST_TRADE_URL = "https://www.twse.com.tw/rwd/zh/fund/T86?response=json"
TPEX_INST_TRADE_URL = "https://www.tpex.org.tw/rwd/zh/fund/T86?response=json"

# ── 月營收 API ────────────────────────────────────────────
# 公開資訊觀測站
MOPS_REVENUE_URL = "https://mops.twse.com.tw/nas/t21/sii/t21sc03_{year}_{month}_0.html"
MOPS_REVENUE_OTC_URL = "https://mops.twse.com.tw/nas/t21/otc/t21sc03_{year}_{month}_0.html"

# ── Google Trends ─────────────────────────────────────────
TRENDS_TIMEFRAME = "today 3-m"
TRENDS_GEO = "TW"
TRENDS_ACCELERATION_THRESHOLD = 1.5
TRENDS_QUERY_DELAY = 3

# ── Claude API ────────────────────────────────────────────
CLAUDE_MODEL = "claude-sonnet-4-20250514"
CLAUDE_MAX_TOKENS = 1000
THEME_CACHE_DAYS = 30

# ── 快取路徑 ──────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(BASE_DIR, "cache")
SCORES_DIR = os.path.join(CACHE_DIR, "scores")
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(SCORES_DIR, exist_ok=True)
