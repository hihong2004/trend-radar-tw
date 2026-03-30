"""
scoring/trend_consistency.py — 維度 6：趨勢持續性
衡量趨勢的品質（穩定上漲 vs 暴漲暴跌）。
"""

import pandas as pd
import numpy as np


def score_trend_consistency(close: pd.Series) -> dict:
    """
    評估趨勢穩定度。

    Parameters
    ----------
    close : Series of daily close prices (sorted by date, >= 60 entries)

    Returns
    -------
    dict: {"score": float, "up_week_ratio": float, "pullback_depth": float, "details": str}
    """
    result = {
        "score": 0,
        "up_week_ratio": 0,
        "pullback_depth": 0,
        "above_20ma_ratio": 0,
        "details": "",
    }

    if len(close) < 60:
        result["details"] = "數據不足"
        return result

    # 近 12 週上漲週數比例
    weekly = close.resample("W-FRI").last().dropna()
    if len(weekly) >= 13:
        recent_12w = weekly.iloc[-13:]
        weekly_returns = recent_12w.pct_change().dropna()
        up_weeks = (weekly_returns > 0).sum()
        total_weeks = len(weekly_returns)
        up_week_ratio = up_weeks / total_weeks if total_weeks > 0 else 0
    else:
        up_week_ratio = 0.5  # default

    # 近 60 日最大回撤
    recent_60 = close.iloc[-60:]
    running_max = recent_60.cummax()
    drawdown = (recent_60 / running_max - 1)
    max_pullback = drawdown.min()  # 負值

    # 近 60 日在 20MA 以上的天數比例
    ma20 = close.rolling(20).mean()
    recent_60_close = close.iloc[-60:]
    recent_60_ma = ma20.iloc[-60:]
    above_20ma = (recent_60_close > recent_60_ma).sum()
    above_20ma_ratio = above_20ma / 60

    result["up_week_ratio"] = round(up_week_ratio, 2)
    result["pullback_depth"] = round(max_pullback * 100, 1)  # 百分比
    result["above_20ma_ratio"] = round(above_20ma_ratio, 2)

    # 評分
    score = 0
    if up_week_ratio > 0.7 and max_pullback > -0.10:
        score = 95  # 穩定上漲，回撤淺
    elif up_week_ratio > 0.7 and max_pullback > -0.15:
        score = 85
    elif up_week_ratio > 0.6 and max_pullback > -0.15:
        score = 75
    elif up_week_ratio > 0.6 and max_pullback > -0.20:
        score = 65
    elif up_week_ratio > 0.5:
        score = 55
    elif up_week_ratio > 0.4:
        score = 40
    else:
        score = 20

    # above_20ma 加分/扣分
    if above_20ma_ratio > 0.8:
        score = min(100, score + 5)
    elif above_20ma_ratio < 0.3:
        score = max(0, score - 10)

    result["score"] = score
    result["details"] = (
        f"漲週比:{up_week_ratio:.0%} "
        f"最大回撤:{max_pullback*100:.1f}% "
        f"在20MA上:{above_20ma_ratio:.0%}"
    )
    return result
