"""
data_pipeline.py — 台股數據下載
1. yfinance 批量 OHLCV（.TW / .TWO）
2. 證交所三大法人買賣超
3. 公開資訊觀測站月營收
"""

import time
import logging
import pandas as pd
import numpy as np
import requests
import os
import re
from datetime import datetime, timedelta
from io import StringIO

import yfinance as yf
import config

logger = logging.getLogger(__name__)

OHLCV_CACHE = os.path.join(config.CACHE_DIR, "ohlcv_tw.parquet")
BENCHMARK_CACHE = os.path.join(config.CACHE_DIR, "benchmark_tw.parquet")
INST_CACHE = os.path.join(config.CACHE_DIR, "institutional_tw.parquet")
REVENUE_CACHE = os.path.join(config.CACHE_DIR, "revenue_tw.parquet")

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}


# ═══════════════════════════════════════════════════════════
# OHLCV 下載
# ═══════════════════════════════════════════════════════════

def _download_batch(tickers: list, start: str, end: str = None) -> pd.DataFrame:
    """下載一批 ticker 的 OHLCV"""
    try:
        data = yf.download(
            tickers, start=start, end=end,
            auto_adjust=True, progress=False, timeout=60, threads=True,
        )
    except Exception as e:
        logger.error(f"yfinance 下載失敗: {e}")
        return pd.DataFrame()

    if data.empty:
        return pd.DataFrame()

    frames = []
    price_cols = ["Open", "High", "Low", "Close", "Volume"]

    if isinstance(data.columns, pd.MultiIndex):
        available = data.columns.get_level_values(1).unique()
        for ticker in available:
            try:
                df_t = data.xs(ticker, level=1, axis=1).copy()
                df_t = df_t[[c for c in price_cols if c in df_t.columns]]
                df_t.columns = [c.lower() for c in df_t.columns]
                df_t["ticker"] = ticker
                df_t.index.name = "date"
                df_t = df_t.reset_index()
                frames.append(df_t)
            except Exception:
                continue
    else:
        data.columns = [c.lower() if isinstance(c, str) else c for c in data.columns]
        if len(tickers) == 1:
            data["ticker"] = tickers[0]
        data.index.name = "date"
        data = data.reset_index()
        frames.append(data)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result["date"] = pd.to_datetime(result["date"])
    return result


def _batch_download(tickers: list, start_date: str) -> pd.DataFrame:
    """分批下載"""
    batch_size = config.DATA_BATCH_SIZE
    all_frames = []
    total_batches = (len(tickers) + batch_size - 1) // batch_size

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        batch_num = i // batch_size + 1
        logger.info(f"  批次 {batch_num}/{total_batches}: {len(batch)} 檔")

        try:
            df = _download_batch(batch, start=start_date)
            if not df.empty:
                all_frames.append(df)
                logger.info(f"    ✅ {df['ticker'].nunique()} 檔")
        except Exception as e:
            logger.error(f"    ❌ 失敗: {e}")

        if batch_num < total_batches:
            time.sleep(config.DATA_BATCH_DELAY)

    if not all_frames:
        return pd.DataFrame()

    result = pd.concat(all_frames, ignore_index=True)
    result = result.drop_duplicates(subset=["date", "ticker"], keep="last")
    logger.info(f"✅ 共 {result['ticker'].nunique()} 檔, {len(result)} 筆")
    return result


def download_all_ohlcv(tickers: list, full_refresh: bool = False) -> pd.DataFrame:
    """下載所有 OHLCV，支援增量更新"""
    if not full_refresh and os.path.exists(OHLCV_CACHE):
        cached = pd.read_parquet(OHLCV_CACHE)
        cached["date"] = pd.to_datetime(cached["date"])
        last_date = cached["date"].max()
        days_behind = (datetime.now() - last_date).days

        if days_behind <= 1:
            logger.info(f"  快取已是最新（{last_date.date()}）")
            return cached

        start = (last_date - timedelta(days=config.INCREMENTAL_DAYS)).strftime("%Y-%m-%d")
        logger.info(f"🔄 增量更新：從 {start}")
        new_data = _batch_download(tickers, start)

        if not new_data.empty:
            cutoff = pd.to_datetime(start)
            old = cached[cached["date"] < cutoff]
            combined = pd.concat([old, new_data], ignore_index=True)
            combined = combined.drop_duplicates(subset=["date", "ticker"], keep="last")
            combined = combined.sort_values(["ticker", "date"]).reset_index(drop=True)
        else:
            combined = cached

        _save(combined, OHLCV_CACHE)
        return combined

    years = config.DATA_HISTORY_YEARS
    start = (datetime.now() - timedelta(days=years * 365)).strftime("%Y-%m-%d")
    logger.info(f"🔄 完整下載：{len(tickers)} 檔，從 {start}")
    result = _batch_download(tickers, start)
    _save(result, OHLCV_CACHE)
    return result


def download_benchmark(full_refresh: bool = False) -> pd.DataFrame:
    """下載 0050.TW 基準"""
    if not full_refresh and os.path.exists(BENCHMARK_CACHE):
        cached = pd.read_parquet(BENCHMARK_CACHE)
        last = pd.to_datetime(cached["date"]).max()
        if (datetime.now() - last).days <= 1:
            return cached

    start = (datetime.now() - timedelta(days=config.DATA_HISTORY_YEARS * 365)).strftime("%Y-%m-%d")
    df = _download_batch([config.BENCHMARK], start)
    _save(df, BENCHMARK_CACHE)
    return df


# ═══════════════════════════════════════════════════════════
# 三大法人買賣超
# ═══════════════════════════════════════════════════════════

def fetch_institutional_trading(date_str: str = None) -> pd.DataFrame:
    """
    從證交所取得三大法人買賣超日報。

    Returns DataFrame: [stock_id, foreign_buy, foreign_sell, foreign_net,
                        trust_buy, trust_sell, trust_net,
                        dealer_net, total_net, date]
    """
    if date_str is None:
        date_str = datetime.now().strftime("%Y%m%d")

    try:
        url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str}&selectType=ALLBUT0999&response=json"
        resp = requests.get(url, headers=_HEADERS, timeout=30)
        data = resp.json()

        if data.get("stat") != "OK" or "data" not in data:
            logger.warning(f"法人資料無數據: {date_str}")
            return pd.DataFrame()

        rows = data["data"]
        records = []

        for row in rows:
            # 清理數字中的逗號
            clean = [str(x).replace(",", "").strip() for x in row]
            try:
                stock_id = clean[0].strip()
                if not re.match(r"^\d{4}$", stock_id):
                    continue

                records.append({
                    "stock_id": stock_id,
                    "date": date_str,
                    "foreign_net": _safe_int(clean[4]),
                    "trust_net": _safe_int(clean[10]),
                    "dealer_net": _safe_int(clean[11]),
                    "total_net": _safe_int(clean[18]) if len(clean) > 18 else 0,
                })
            except (IndexError, ValueError):
                continue

        df = pd.DataFrame(records)
        logger.info(f"  法人資料 {date_str}: {len(df)} 筆")
        return df

    except Exception as e:
        logger.error(f"法人資料取得失敗 {date_str}: {e}")
        return pd.DataFrame()


def fetch_institutional_history(days: int = 30) -> pd.DataFrame:
    """取得近 N 天的法人買賣超歷史"""
    all_frames = []

    for i in range(days):
        dt = datetime.now() - timedelta(days=i)
        if dt.weekday() >= 5:
            continue
        date_str = dt.strftime("%Y%m%d")
        df = fetch_institutional_trading(date_str)
        if not df.empty:
            all_frames.append(df)
        time.sleep(1)

    if not all_frames:
        return pd.DataFrame()

    result = pd.concat(all_frames, ignore_index=True)
    return result


def load_institutional_data(full_refresh: bool = False) -> pd.DataFrame:
    """載入法人資料（含快取）"""
    if not full_refresh and os.path.exists(INST_CACHE):
        cached = pd.read_parquet(INST_CACHE)
        if not cached.empty:
            last = cached["date"].max()
            today = datetime.now().strftime("%Y%m%d")
            if last >= today:
                logger.info("📦 法人資料使用快取")
                return cached

    logger.info("🔄 抓取法人買賣超（近 20 天）...")
    df = fetch_institutional_history(days=20)
    if not df.empty:
        _save(df, INST_CACHE)
    return df


# ═══════════════════════════════════════════════════════════
# 月營收
# ═══════════════════════════════════════════════════════════

def fetch_monthly_revenue(year: int, month: int, market: str = "sii") -> pd.DataFrame:
    """
    從公開資訊觀測站取得月營收。
    market: "sii" (上市) 或 "otc" (上櫃)
    """
    tw_year = year - 1911

    # 使用 POST 方式，比較穩定
    url = "https://mops.twse.com.tw/nas/t21/{market}/t21sc03_{tw_year}_{month}_0.html"
    url = url.format(market=market, tw_year=tw_year, month=month)

    try:
        resp = requests.get(url, headers={
            **_HEADERS,
            "Referer": "https://mops.twse.com.tw/mops/web/t21sc03",
        }, timeout=30)
        resp.encoding = "big5"

        if resp.status_code != 200 or len(resp.text) < 500:
            logger.debug(f"  營收 {year}/{month} {market}: 無資料")
            return pd.DataFrame()

        tables = pd.read_html(StringIO(resp.text), encoding="big5")

        all_records = []
        for t in tables:
            if len(t.columns) < 5:
                continue
            # 重新命名欄位（用位置）
            cols = list(t.columns)
            for idx, row in t.iterrows():
                try:
                    sid = str(row.iloc[0]).strip()
                    if not re.match(r"^\d{4}$", sid):
                        continue
                    revenue = float(str(row.iloc[2]).replace(",", ""))
                    yoy = None
                    # YoY 通常在第 5 或第 6 欄
                    for col_idx in [5, 6, 7]:
                        if col_idx < len(row):
                            try:
                                yoy = float(str(row.iloc[col_idx]).replace(",", ""))
                                break
                            except (ValueError, TypeError):
                                continue

                    all_records.append({
                        "stock_id": sid,
                        "year": year,
                        "month": month,
                        "revenue": revenue,
                        "yoy_pct": yoy,
                    })
                except (ValueError, IndexError):
                    continue

        if all_records:
            logger.info(f"  營收 {year}/{month} {market}: {len(all_records)} 筆")
        return pd.DataFrame(all_records)

    except Exception as e:
        logger.debug(f"  營收 {year}/{month} {market}: {e}")
        return pd.DataFrame()


def fetch_recent_revenue(months: int = 6) -> pd.DataFrame:
    """取得近 N 個月的月營收"""
    all_frames = []
    now = datetime.now()

    for i in range(months):
        dt = now - timedelta(days=30 * (i + 1))
        year = dt.year
        month = dt.month

        for market in ["sii", "otc"]:
            df = fetch_monthly_revenue(year, month, market)
            if not df.empty:
                all_frames.append(df)
            time.sleep(1)

    if not all_frames:
        return pd.DataFrame()

    result = pd.concat(all_frames, ignore_index=True)
    result = result.drop_duplicates(subset=["stock_id", "year", "month"], keep="last")
    return result


def load_revenue_data(full_refresh: bool = False) -> pd.DataFrame:
    """載入營收資料（含快取）"""
    if not full_refresh and os.path.exists(REVENUE_CACHE):
        cached = pd.read_parquet(REVENUE_CACHE)
        if not cached.empty:
            logger.info("📦 營收資料使用快取")
            return cached

    logger.info("🔄 抓取月營收（近 6 個月）...")
    df = fetch_recent_revenue(months=6)
    if not df.empty:
        _save(df, REVENUE_CACHE)
    return df


# ═══════════════════════════════════════════════════════════
# 工具函式
# ═══════════════════════════════════════════════════════════

def _safe_int(val) -> int:
    try:
        return int(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return 0


def _save(df: pd.DataFrame, path: str):
    if df.empty:
        return
    try:
        df.to_parquet(path, index=False)
        logger.info(f"💾 {os.path.basename(path)}")
    except Exception as e:
        logger.warning(f"快取失敗: {e}")


def get_ticker_df(ohlcv: pd.DataFrame, ticker: str) -> pd.DataFrame:
    df = ohlcv[ohlcv["ticker"] == ticker].copy()
    return df.sort_values("date").reset_index(drop=True)


def load_all_data(full_refresh: bool = False) -> dict:
    """主入口：載入所有數據"""
    from universe_tw import get_all_tickers, get_sector_map, get_name_map

    tickers = get_all_tickers()
    sector_map = get_sector_map()
    name_map = get_name_map()

    logger.info(f"📊 Universe: {len(tickers)} 檔")

    benchmark = download_benchmark(full_refresh)
    logger.info(f"📊 Benchmark (0050.TW): {len(benchmark)} 筆")

    ohlcv = download_all_ohlcv(tickers, full_refresh)
    logger.info(f"📊 OHLCV: {ohlcv['ticker'].nunique() if not ohlcv.empty else 0} 檔")

    inst_data = load_institutional_data(full_refresh)
    logger.info(f"📊 法人資料: {len(inst_data)} 筆")

    revenue_data = load_revenue_data(full_refresh)
    logger.info(f"📊 營收資料: {len(revenue_data)} 筆")

    return {
        "ohlcv": ohlcv,
        "benchmark": benchmark,
        "tickers": tickers,
        "sector_map": sector_map,
        "name_map": name_map,
        "institutional": inst_data,
        "revenue": revenue_data,
    }
