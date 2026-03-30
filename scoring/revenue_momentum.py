"""
scoring/revenue_momentum.py — 維度 9：月營收動能（台股特有）
追蹤月營收年增率加速度、連續創高等訊號。
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def score_revenue_momentum(
    stock_id: str,
    revenue_data: pd.DataFrame,
) -> dict:
    """
    評估月營收動能。

    Parameters
    ----------
    stock_id : e.g. "2330"
    revenue_data : DataFrame [stock_id, year, month, revenue, yoy_pct]

    Returns
    -------
    dict: {
        "score": float (0-100),
        "latest_yoy": float or None,
        "yoy_accelerating": bool,
        "consecutive_growth": int,
        "details": str,
    }
    """
    result = {
        "score": 30,  # 預設中性
        "latest_yoy": None,
        "yoy_accelerating": False,
        "consecutive_growth": 0,
        "details": "無營收資料",
    }

    if revenue_data.empty or "stock_id" not in revenue_data.columns:
        return result

    df = revenue_data[revenue_data["stock_id"] == stock_id].copy()
    if df.empty or len(df) < 2:
        return result

    # 按年月排序
    df = df.sort_values(["year", "month"]).reset_index(drop=True)

    # 最新一筆 YoY
    latest = df.iloc[-1]
    latest_yoy = latest.get("yoy_pct")

    if pd.isna(latest_yoy):
        # 如果沒有 yoy_pct，嘗試自行計算
        if len(df) >= 13:
            current_rev = df.iloc[-1]["revenue"]
            year_ago_rev = df.iloc[-13]["revenue"] if len(df) >= 13 else None
            if year_ago_rev and year_ago_rev > 0:
                latest_yoy = (current_rev / year_ago_rev - 1) * 100

    result["latest_yoy"] = round(float(latest_yoy), 1) if pd.notna(latest_yoy) else None

    # 營收 YoY 加速度：最新 vs 前一期
    yoy_accelerating = False
    if len(df) >= 3 and "yoy_pct" in df.columns:
        recent_yoys = df["yoy_pct"].dropna().tail(3)
        if len(recent_yoys) >= 2:
            yoy_accelerating = recent_yoys.iloc[-1] > recent_yoys.iloc[-2]

    result["yoy_accelerating"] = yoy_accelerating

    # 連續正成長月數
    consecutive = 0
    if "yoy_pct" in df.columns:
        yoys = df["yoy_pct"].dropna().values
        for v in reversed(yoys):
            if v > 0:
                consecutive += 1
            else:
                break

    result["consecutive_growth"] = consecutive

    # 營收是否創歷史新高（近 12 個月最高）
    revenue_new_high = False
    if "revenue" in df.columns and len(df) >= 2:
        recent_rev = df["revenue"].iloc[-1]
        prev_max = df["revenue"].iloc[:-1].max()
        revenue_new_high = recent_rev >= prev_max

    # ── 評分 ──
    score = 30

    # YoY 成長率
    if pd.notna(latest_yoy):
        if latest_yoy > 50:
            score += 30
        elif latest_yoy > 30:
            score += 25
        elif latest_yoy > 15:
            score += 18
        elif latest_yoy > 5:
            score += 10
        elif latest_yoy > 0:
            score += 5
        elif latest_yoy > -10:
            score -= 5
        else:
            score -= 15

    # 加速度加分
    if yoy_accelerating:
        score += 10

    # 連續正成長加分
    if consecutive >= 6:
        score += 10
    elif consecutive >= 3:
        score += 5

    # 營收創新高
    if revenue_new_high:
        score += 10

    score = max(0, min(100, score))
    result["score"] = score

    # 格式化
    yoy_str = f"YoY:{latest_yoy:+.1f}%" if pd.notna(latest_yoy) else "YoY:N/A"
    acc_str = "↑加速" if yoy_accelerating else ""
    consec_str = f"連{consecutive}月+" if consecutive > 0 else ""
    high_str = "創高" if revenue_new_high else ""

    result["details"] = f"{yoy_str} {acc_str} {consec_str} {high_str}".strip()
    return result
