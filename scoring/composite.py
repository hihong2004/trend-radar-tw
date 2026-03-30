"""
scoring/composite.py — 九維總分合成 + Stage 判定（台股版）
"""

import pandas as pd
import numpy as np
import logging
import os
import json
from datetime import datetime

import config
from data_pipeline import get_ticker_df
from scoring.relative_strength import score_relative_strength, compute_all_raw_rs
from scoring.price_structure import score_price_structure
from scoring.volume_analysis import score_volume
from scoring.volatility import score_volatility
from scoring.sector_momentum import score_sector_momentum
from scoring.trend_consistency import score_trend_consistency
from scoring.theme_momentum import score_theme_momentum
from scoring.institutional_flow import score_institutional_flow
from scoring.revenue_momentum import score_revenue_momentum

logger = logging.getLogger(__name__)

STAGE_HISTORY_PATH = os.path.join(config.CACHE_DIR, "stage_history.json")


def score_single_ticker(
    ticker: str,
    ohlcv: pd.DataFrame,
    benchmark_close: pd.Series,
    all_rs_scores: pd.Series,
    sector_map: dict,
    name_map: dict,
    inst_data: pd.DataFrame,
    revenue_data: pd.DataFrame,
) -> dict:
    df = get_ticker_df(ohlcv, ticker)
    if df.empty or len(df) < 60:
        return None

    close = df.set_index("date")["close"]
    volume = df.set_index("date")["volume"]
    sector = sector_map.get(ticker, "")
    name = name_map.get(ticker, "")
    stock_id = ticker.split(".")[0]

    # 過濾低成交量
    avg_vol = volume.tail(20).mean()
    if pd.notna(avg_vol) and avg_vol < config.MIN_DAILY_VOLUME * 1000:
        return None

    bench_aligned = benchmark_close.reindex(close.index).ffill()

    # ── 九維評分 ──
    d1 = score_relative_strength(close, bench_aligned, all_rs_scores)
    d2 = score_price_structure(close)
    d3 = score_volume(close, volume)
    d4 = score_volatility(close)
    d5 = score_sector_momentum(ticker, sector, all_rs_scores, sector_map)
    d6 = score_trend_consistency(close)
    d7 = score_theme_momentum(ticker)
    d8 = score_institutional_flow(stock_id, inst_data)
    d9 = score_revenue_momentum(stock_id, revenue_data)

    # ── 加權總分 ──
    w = config.SCORING_WEIGHTS
    total = (
        d1["score"] * w["relative_strength"]
        + d2["score"] * w["price_structure"]
        + d3["score"] * w["volume_analysis"]
        + d4["score"] * w["volatility"]
        + d5["score"] * w["sector_momentum"]
        + d6["score"] * w["trend_consistency"]
        + d7["score"] * w["theme_momentum"]
        + d8["score"] * w["institutional_flow"]
        + d9["score"] * w["revenue_momentum"]
    )
    total = round(min(total, 100), 1)

    stars = 1
    for s, threshold in sorted(config.RATING_THRESHOLDS.items(), reverse=True):
        if total >= threshold:
            stars = s
            break

    return {
        "ticker": ticker,
        "stock_id": stock_id,
        "name": name,
        "sector": sector,
        "total_score": total,
        "stars": stars,
        "price": round(float(close.iloc[-1]), 2),
        "dimensions": {
            "relative_strength": d1["score"],
            "price_structure": d2["score"],
            "volume_analysis": d3["score"],
            "volatility": d4["score"],
            "sector_momentum": d5["score"],
            "trend_consistency": d6["score"],
            "theme_momentum": d7["score"],
            "institutional_flow": d8["score"],
            "revenue_momentum": d9["score"],
        },
        "details": {
            "rs": d1["details"],
            "price": d2["details"],
            "volume": d3["details"],
            "volatility": d4["details"],
            "sector": d5["details"],
            "consistency": d6["details"],
            "theme": d7["details"],
            "institutional": d8["details"],
            "revenue": d9["details"],
        },
        "institutional": {
            "foreign_consec": d8.get("foreign_consecutive", 0),
            "trust_consec": d8.get("trust_consecutive", 0),
            "both_buying": d8.get("both_buying", False),
        },
        "revenue_info": {
            "latest_yoy": d9.get("latest_yoy"),
            "accelerating": d9.get("yoy_accelerating", False),
        },
        "themes": d7.get("themes", []),
        "concept_group": d5.get("concept_group", ""),
    }


def score_all_tickers(data: dict) -> pd.DataFrame:
    ohlcv = data["ohlcv"]
    benchmark = data["benchmark"]
    tickers = data["tickers"]
    sector_map = data["sector_map"]
    name_map = data.get("name_map", {})
    inst_data = data.get("institutional", pd.DataFrame())
    revenue_data = data.get("revenue", pd.DataFrame())

    if ohlcv.empty:
        logger.error("OHLCV 為空")
        return pd.DataFrame()

    bench_df = benchmark[benchmark["ticker"] == config.BENCHMARK].sort_values("date")
    benchmark_close = bench_df.set_index("date")["close"]

    logger.info("📊 計算全體 RS...")
    all_rs_scores = compute_all_raw_rs(ohlcv, benchmark_close)
    logger.info(f"  {len(all_rs_scores)} 檔有 RS")

    logger.info(f"📊 開始評分 {len(tickers)} 檔...")
    results = []
    scored = 0
    skipped = 0

    for i, ticker in enumerate(tickers):
        try:
            r = score_single_ticker(
                ticker, ohlcv, benchmark_close, all_rs_scores,
                sector_map, name_map, inst_data, revenue_data,
            )
            if r:
                results.append(r)
                scored += 1
            else:
                skipped += 1
        except Exception as e:
            logger.debug(f"  {ticker} 失敗: {e}")
            skipped += 1

        if (i + 1) % 200 == 0:
            logger.info(f"  進度: {i+1}/{len(tickers)}")

    logger.info(f"✅ 評分: {scored} 成功, {skipped} 跳過")

    if not results:
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df = df.sort_values("total_score", ascending=False).reset_index(drop=True)
    return df


def determine_stages(scores_df: pd.DataFrame) -> pd.DataFrame:
    if scores_df.empty:
        return scores_df

    params = config.STAGE_PARAMS
    history = _load_stage_history()
    stages = []
    today = datetime.now().strftime("%Y-%m-%d")

    for _, row in scores_df.iterrows():
        ticker = row["ticker"]
        total = row["total_score"]
        sector_score = row["dimensions"]["sector_momentum"]

        prev = history.get(ticker, {})
        prev_stage = prev.get("stage", 0)
        prev_high = prev.get("high_score", 0)
        days_above_70 = prev.get("days_above_70", 0)
        first_seen = prev.get("first_seen", today)

        high_score = max(prev_high, total)

        if total >= params["acceleration_threshold"]:
            days_above_70 += 1
        else:
            days_above_70 = 0

        if total < params["awakening_threshold"]:
            stage = 0
        elif (
            total >= params["acceleration_threshold"]
            and days_above_70 >= params["acceleration_hold_days"]
            and sector_score >= params["sector_confirm_threshold"]
        ):
            stage = 2
        elif total >= params["awakening_threshold"]:
            stage = 1
        else:
            stage = 0

        if prev_stage == 2:
            if high_score - total > params["decay_drop_points"]:
                stage = 3
            if row["dimensions"]["relative_strength"] < params["rs_decay_percentile"]:
                stage = 3

        stage_changed = stage != prev_stage
        transition = f"{prev_stage}→{stage}" if stage_changed else ""

        stages.append({
            "stage": stage,
            "stage_changed": stage_changed,
            "transition": transition,
            "days_above_70": days_above_70,
            "high_score": high_score,
            "first_seen": first_seen if total >= params["awakening_threshold"] else "",
        })

        history[ticker] = {
            "stage": stage, "high_score": high_score,
            "days_above_70": days_above_70,
            "first_seen": first_seen if total >= params["awakening_threshold"] else "",
            "last_updated": today,
        }

    _save_stage_history(history)
    stage_df = pd.DataFrame(stages)
    return pd.concat([scores_df.reset_index(drop=True), stage_df], axis=1)


def get_watchlist(scored_df, min_stars=3):
    if scored_df.empty:
        return scored_df
    return scored_df[scored_df["stars"] >= min_stars].copy()


def get_stage_transitions(scored_df):
    if scored_df.empty or "stage_changed" not in scored_df.columns:
        return {"new_stage1": [], "new_stage2": [], "decay_stage3": []}
    changed = scored_df[scored_df["stage_changed"] == True]
    return {
        "new_stage1": changed[changed["transition"].str.contains("→1")]["ticker"].tolist(),
        "new_stage2": changed[changed["transition"].str.contains("→2")]["ticker"].tolist(),
        "decay_stage3": changed[changed["transition"].str.contains("→3")]["ticker"].tolist(),
    }


def save_daily_snapshot(scored_df):
    if scored_df.empty:
        return
    today = datetime.now().strftime("%Y-%m-%d")
    path = os.path.join(config.SCORES_DIR, f"{today}.parquet")
    cols = ["ticker", "stock_id", "name", "sector", "total_score", "stars", "stage", "price"]
    save_df = scored_df[[c for c in cols if c in scored_df.columns]].copy()
    try:
        save_df.to_parquet(path, index=False)
        logger.info(f"💾 快照: {path}")
    except Exception as e:
        logger.warning(f"快照失敗: {e}")


def _load_stage_history():
    if os.path.exists(STAGE_HISTORY_PATH):
        try:
            with open(STAGE_HISTORY_PATH, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_stage_history(history):
    try:
        with open(STAGE_HISTORY_PATH, "w") as f:
            json.dump(history, f)
    except Exception as e:
        logger.warning(f"Stage 歷史失敗: {e}")
