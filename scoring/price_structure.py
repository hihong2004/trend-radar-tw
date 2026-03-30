"""
scoring/price_structure.py — 維度 2：價格結構
基於 Mark Minervini 的 Trend Template，檢查均線排列與突破狀態。
"""

import pandas as pd
import numpy as np


def score_price_structure(close: pd.Series) -> dict:
    """
    評估價格結構。

    Parameters
    ----------
    close : Series of daily close prices (sorted by date, at least 250 entries)

    Returns
    -------
    dict: {"score": float, "checks": dict, "details": str}
    """
    result = {"score": 0, "checks": {}, "details": ""}

    if len(close) < 250:
        result["details"] = "數據不足（需 250 天）"
        return result

    current = close.iloc[-1]
    ma50 = close.rolling(50).mean().iloc[-1]
    ma150 = close.rolling(150).mean().iloc[-1]
    ma200 = close.rolling(200).mean().iloc[-1]

    # 200MA 斜率（近 20 日）
    ma200_series = close.rolling(200).mean()
    ma200_now = ma200_series.iloc[-1]
    ma200_20ago = ma200_series.iloc[-21] if len(ma200_series) > 20 else ma200_now
    ma200_slope_up = ma200_now > ma200_20ago

    # 52 週（約 252 交易日）高低點
    high_52w = close.iloc[-252:].max() if len(close) >= 252 else close.max()
    low_52w = close.iloc[-252:].min() if len(close) >= 252 else close.min()

    points = 0
    checks = {}

    # 檢查項目
    c1 = current > ma50
    checks["above_50ma"] = c1
    if c1:
        points += 15

    c2 = current > ma150
    checks["above_150ma"] = c2
    if c2:
        points += 15

    c3 = current > ma200
    checks["above_200ma"] = c3
    if c3:
        points += 15

    c4 = ma50 > ma150 > ma200
    checks["ma_bullish_alignment"] = c4
    if c4:
        points += 25

    c5 = ma200_slope_up
    checks["ma200_slope_up"] = c5
    if c5:
        points += 10

    c6 = current >= high_52w * 0.75
    checks["within_25pct_of_52w_high"] = c6
    if c6:
        points += 10

    c7 = current >= high_52w * 0.95
    checks["near_52w_high"] = c7
    if c7:
        points += 10

    score = min(points, 100)
    passed = sum(1 for v in checks.values() if v)

    result["score"] = score
    result["checks"] = checks
    result["details"] = f"{passed}/7 條件通過"

    return result
