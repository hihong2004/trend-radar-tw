"""
performance_tracker.py — 歷史命中率追蹤
追蹤過去被標記為 Stage 1/2 的股票，後來實際表現如何。
用於評估系統的有效性並持續調優。
"""

import os
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from glob import glob

import config

logger = logging.getLogger(__name__)


def load_historical_snapshots(days_back: int = 90) -> dict:
    """
    載入過去 N 天的每日評分快照。

    Returns
    -------
    dict: {date_str: DataFrame}
    """
    snapshots = {}
    pattern = os.path.join(config.SCORES_DIR, "*.parquet")
    files = sorted(glob(pattern))

    cutoff = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    for f in files:
        date_str = os.path.basename(f).replace(".parquet", "")
        if date_str < cutoff:
            continue
        try:
            df = pd.read_parquet(f)
            snapshots[date_str] = df
        except Exception as e:
            logger.debug(f"快照載入失敗 {f}: {e}")

    return snapshots


def compute_forward_returns(
    ohlcv: pd.DataFrame,
    ticker: str,
    entry_date: str,
    forward_days: list = None,
) -> dict:
    """
    計算某股票從某日起的前瞻報酬。

    Returns
    -------
    dict: {"30d": float, "60d": float, "90d": float} (百分比)
    """
    if forward_days is None:
        forward_days = [30, 60, 90]

    df = ohlcv[ohlcv["ticker"] == ticker].sort_values("date")
    if df.empty:
        return {}

    df = df.set_index("date")
    entry = pd.to_datetime(entry_date)

    # 找到最近的交易日
    valid_dates = df.index[df.index >= entry]
    if valid_dates.empty:
        return {}

    actual_entry = valid_dates[0]
    entry_price = df.loc[actual_entry, "close"]

    results = {}
    for days in forward_days:
        target_date = actual_entry + timedelta(days=days)
        future_dates = df.index[df.index >= target_date]
        if future_dates.empty:
            results[f"{days}d"] = None
        else:
            exit_price = df.loc[future_dates[0], "close"]
            ret = (exit_price / entry_price - 1) * 100
            results[f"{days}d"] = round(float(ret), 2)

    return results


def track_performance(ohlcv: pd.DataFrame) -> pd.DataFrame:
    """
    追蹤所有歷史 Stage 1/2 訊號的後續表現。

    Returns
    -------
    DataFrame with columns:
      ticker, signal_date, stage, score, sector,
      return_30d, return_60d, return_90d
    """
    snapshots = load_historical_snapshots(days_back=180)

    if not snapshots:
        logger.info("無歷史快照可追蹤")
        return pd.DataFrame()

    records = []
    seen = set()  # 避免同一股票同一 stage 重複計算

    for date_str, df in sorted(snapshots.items()):
        if "stage" not in df.columns:
            continue

        # 只追蹤 Stage 1 和 Stage 2 的首次出現
        for _, row in df.iterrows():
            stage = row.get("stage", 0)
            if stage not in (1, 2):
                continue

            ticker = row["ticker"]
            key = f"{ticker}_{stage}_{date_str[:7]}"  # 同月份只算一次
            if key in seen:
                continue
            seen.add(key)

            # 計算前瞻報酬
            returns = compute_forward_returns(ohlcv, ticker, date_str)

            records.append({
                "ticker": ticker,
                "signal_date": date_str,
                "stage": stage,
                "score": row.get("total_score", 0),
                "sector": row.get("sector", ""),
                "return_30d": returns.get("30d"),
                "return_60d": returns.get("60d"),
                "return_90d": returns.get("90d"),
            })

    if not records:
        return pd.DataFrame()

    result = pd.DataFrame(records)
    return result


def compute_hit_rates(perf_df: pd.DataFrame) -> dict:
    """
    計算系統的歷史命中率統計。

    Returns
    -------
    dict with hit rate summaries
    """
    if perf_df.empty:
        return {"message": "尚無足夠歷史數據"}

    stats = {}

    for stage in [1, 2]:
        subset = perf_df[perf_df["stage"] == stage]
        if subset.empty:
            continue

        stage_stats = {"count": len(subset)}

        for period in ["30d", "60d", "90d"]:
            col = f"return_{period}"
            valid = subset[col].dropna()
            if len(valid) == 0:
                continue

            stage_stats[period] = {
                "samples": len(valid),
                "win_rate": round((valid > 0).mean() * 100, 1),
                "median_return": round(float(valid.median()), 1),
                "mean_return": round(float(valid.mean()), 1),
                "best": round(float(valid.max()), 1),
                "worst": round(float(valid.min()), 1),
            }

        stats[f"stage_{stage}"] = stage_stats

    return stats


def format_performance_summary(stats: dict) -> str:
    """格式化命中率為文字摘要"""
    lines = ["📊 系統歷史表現追蹤", ""]

    if "message" in stats:
        lines.append(stats["message"])
        return "\n".join(lines)

    for key in ["stage_1", "stage_2"]:
        if key not in stats:
            continue

        s = stats[key]
        stage_num = key.split("_")[1]
        emoji = "🌅" if stage_num == "1" else "🚀"
        lines.append(f"{emoji} Stage {stage_num} 訊號 (共 {s['count']} 次)")

        for period in ["30d", "60d", "90d"]:
            if period not in s:
                continue
            p = s[period]
            lines.append(
                f"  {period}: 勝率 {p['win_rate']}% | "
                f"中位數 {p['median_return']:+.1f}% | "
                f"N={p['samples']}"
            )

        lines.append("")

    return "\n".join(lines)
