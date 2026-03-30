"""
universe_tw.py — 台股成分股清單
從證交所 Open API + 櫃買中心取得上市櫃股票清單與產業分類。
"""

import logging
import pandas as pd
import requests
import json
import os
from datetime import datetime

import config

logger = logging.getLogger(__name__)

UNIVERSE_CACHE = os.path.join(config.CACHE_DIR, "universe_tw.json")

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}

# 證交所產業代碼 → 中文名稱對照表
SECTOR_CODE_MAP = {
    "01": "水泥工業", "02": "食品工業", "03": "塑膠工業",
    "04": "紡織纖維", "05": "電機機械", "06": "電器電纜",
    "07": "化學生技醫療", "21": "化學工業", "22": "生技醫療業",
    "08": "玻璃陶瓷", "09": "造紙工業", "10": "鋼鐵工業",
    "11": "橡膠工業", "12": "汽車工業", "13": "電子工業",
    "14": "建材營造業", "15": "航運業", "16": "觀光餐旅",
    "17": "金融保險業", "18": "貿易百貨業", "19": "綜合",
    "20": "其他", "23": "油電燃氣業", "24": "半導體業",
    "25": "電腦及週邊設備業", "26": "光電業", "27": "通信網路業",
    "28": "電子零組件業", "29": "電子通路業", "30": "資訊服務業",
    "31": "其他電子業", "32": "文化創意業", "33": "農業科技業",
    "34": "電商", "35": "綠能環保", "36": "數位雲端",
    "37": "運動休閒", "38": "居家生活", "80": "管理股票",
}


def _translate_sector(sector_val: str) -> str:
    """將產業代碼轉為中文名稱"""
    sector_val = str(sector_val).strip()
    # 如果已經是中文，直接回傳
    if any('\u4e00' <= c <= '\u9fff' for c in sector_val):
        return sector_val
    # 嘗試用代碼對照
    if sector_val in SECTOR_CODE_MAP:
        return SECTOR_CODE_MAP[sector_val]
    # 嘗試去掉前導零
    if sector_val.lstrip("0") in SECTOR_CODE_MAP:
        return SECTOR_CODE_MAP[sector_val.lstrip("0")]
    return sector_val


def fetch_twse_stocks() -> pd.DataFrame:
    """從證交所取得上市股票清單與產業分類"""
    try:
        resp = requests.get(config.TWSE_STOCK_LIST_URL, headers=_HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        df = pd.DataFrame(data)

        logger.info(f"  證交所 API 欄位: {list(df.columns)}")

        # 嘗試多種可能的欄位名稱
        col_map = {}
        for c in df.columns:
            cl = str(c)
            # 代號
            if any(kw in cl for kw in ["代號", "公司代號", "股票代號", "證券代號"]):
                col_map[c] = "stock_id"
            # 名稱（公司簡稱 / 公司名稱 / 名稱 等）
            elif any(kw in cl for kw in ["簡稱", "公司簡稱", "名稱", "公司名稱", "股票名稱"]):
                if "name" not in col_map.values():  # 只取第一個匹配
                    col_map[c] = "name"
            # 產業
            elif any(kw in cl for kw in ["產業", "業別", "產業別", "產業類別"]):
                col_map[c] = "sector"

        df = df.rename(columns=col_map)

        logger.info(f"  欄位映射: {col_map}")

        if "stock_id" not in df.columns:
            # 最後手段：如果欄位名完全不符，嘗試用欄位內容判斷
            logger.warning("  欄位名稱映射失敗，嘗試用內容判斷...")
            for c in df.columns:
                sample = df[c].dropna().head(10).astype(str)
                # 找到包含 4 碼數字的欄位
                if sample.str.match(r"^\d{4}$").any():
                    col_map[c] = "stock_id"
                    break
            df = df.rename(columns=col_map)

        if "stock_id" not in df.columns:
            logger.error("  無法識別股票代號欄位")
            return pd.DataFrame(columns=["stock_id", "name", "sector", "market"])

        # 只保留普通股（4碼數字）
        df["stock_id"] = df["stock_id"].astype(str).str.strip()
        df = df[df["stock_id"].str.match(r"^\d{4}$", na=False)]

        for col in ["name", "sector"]:
            if col not in df.columns:
                df[col] = ""
            else:
                df[col] = df[col].astype(str).str.strip()

        df["market"] = "TWSE"
        df["ticker"] = df["stock_id"] + config.TWSE_SUFFIX

        # 記錄幾筆範例
        if len(df) > 0:
            sample = df.head(3)[["stock_id", "name", "sector"]].to_dict("records")
            logger.info(f"  範例: {sample}")

        return df[["stock_id", "ticker", "name", "sector", "market"]]

    except Exception as e:
        logger.error(f"證交所上市清單取得失敗: {e}")
        return pd.DataFrame(columns=["stock_id", "ticker", "name", "sector", "market"])


def fetch_tpex_stocks() -> pd.DataFrame:
    """從櫃買中心取得上櫃股票清單"""
    try:
        resp = requests.get(config.TPEX_STOCK_LIST_URL, headers=_HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        df = pd.DataFrame(data)

        logger.info(f"  櫃買中心 API 欄位: {list(df.columns)}")

        col_map = {}
        for c in df.columns:
            cl = str(c)
            if any(kw in cl for kw in ["代號", "公司代號", "股票代號", "證券代號"]):
                col_map[c] = "stock_id"
            elif any(kw in cl for kw in ["簡稱", "公司簡稱", "名稱", "公司名稱", "股票名稱"]):
                if "name" not in col_map.values():
                    col_map[c] = "name"
            elif any(kw in cl for kw in ["產業", "業別", "產業別", "產業類別"]):
                col_map[c] = "sector"

        df = df.rename(columns=col_map)

        logger.info(f"  欄位映射: {col_map}")

        if "stock_id" not in df.columns:
            logger.warning("  欄位名稱映射失敗，嘗試用內容判斷...")
            for c in df.columns:
                sample = df[c].dropna().head(10).astype(str)
                if sample.str.match(r"^\d{4}$").any():
                    col_map[c] = "stock_id"
                    break
            df = df.rename(columns=col_map)

        if "stock_id" not in df.columns:
            logger.error("  櫃買中心：無法識別股票代號欄位")
            return pd.DataFrame(columns=["stock_id", "ticker", "name", "sector", "market"])

        df = df[df["stock_id"].str.match(r"^\d{4}$", na=False)]

        for col in ["name", "sector"]:
            if col not in df.columns:
                df[col] = ""

        df["market"] = "TPEX"
        df["ticker"] = df["stock_id"] + config.TPEX_SUFFIX

        return df[["stock_id", "ticker", "name", "sector", "market"]]

    except Exception as e:
        logger.error(f"櫃買中心上櫃清單取得失敗: {e}")
        return pd.DataFrame(columns=["stock_id", "ticker", "name", "sector", "market"])


def get_universe(use_cache: bool = True) -> pd.DataFrame:
    """取得台股上市 + 上櫃完整清單"""
    if use_cache and os.path.exists(UNIVERSE_CACHE):
        mod_time = datetime.fromtimestamp(os.path.getmtime(UNIVERSE_CACHE))
        days_old = (datetime.now() - mod_time).days
        if days_old < 30:
            with open(UNIVERSE_CACHE, "r", encoding="utf-8") as f:
                data = json.load(f)
            df = pd.DataFrame(data)
            if len(df) > 0 and "ticker" in df.columns:
                logger.info(f"📦 使用快取 universe ({len(df)} 檔, {days_old} 天前)")
                return df
            else:
                logger.warning("⚠️ 快取為空，重新抓取")

    logger.info("🔄 取得台股成分股清單...")

    twse = fetch_twse_stocks()
    logger.info(f"  上市: {len(twse)} 檔")

    tpex = fetch_tpex_stocks()
    logger.info(f"  上櫃: {len(tpex)} 檔")

    # 確保兩邊欄位一致後再合併
    required_cols = ["stock_id", "ticker", "name", "sector", "market"]

    frames = []
    for df, label in [(twse, "上市"), (tpex, "上櫃")]:
        if df.empty:
            logger.warning(f"  {label} 清單為空，跳過")
            continue
        # 補齊缺失欄位
        for col in required_cols:
            if col not in df.columns:
                df[col] = ""
        frames.append(df[required_cols])

    if not frames:
        logger.error("❌ 上市和上櫃清單都為空")
        return pd.DataFrame(columns=required_cols)

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset="stock_id", keep="first")
    combined = combined.sort_values("stock_id").reset_index(drop=True)

    # 將產業代碼轉為中文名稱
    if "sector" in combined.columns:
        combined["sector"] = combined["sector"].apply(_translate_sector)

    logger.info(f"✅ 合併: {len(combined)} 檔")

    if len(combined) > 0:
        try:
            with open(UNIVERSE_CACHE, "w", encoding="utf-8") as f:
                json.dump(combined.to_dict("records"), f, ensure_ascii=False)
            logger.info("💾 Universe 已快取")
        except Exception as e:
            logger.warning(f"快取儲存失敗: {e}")
    else:
        logger.error("❌ 清單為空，不儲存快取")
        if os.path.exists(UNIVERSE_CACHE):
            os.remove(UNIVERSE_CACHE)

    return combined


def get_all_tickers(use_cache: bool = True) -> list:
    df = get_universe(use_cache)
    if df.empty or "ticker" not in df.columns:
        return []
    return df["ticker"].tolist()


def get_sector_map(use_cache: bool = True) -> dict:
    df = get_universe(use_cache)
    if df.empty:
        return {}
    return dict(zip(df["ticker"], df["sector"]))


def get_name_map(use_cache: bool = True) -> dict:
    df = get_universe(use_cache)
    if df.empty:
        return {}
    return dict(zip(df["ticker"], df["name"]))


def get_stockid_to_ticker(use_cache: bool = True) -> dict:
    """stock_id (e.g. '2330') → ticker (e.g. '2330.TW')"""
    df = get_universe(use_cache)
    if df.empty:
        return {}
    return dict(zip(df["stock_id"], df["ticker"]))
