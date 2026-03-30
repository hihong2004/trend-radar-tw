"""
scoring/volatility.py — 維度 4：波動率突破
偵測「長期盤整壓縮後突然擴張」的形態（Bollinger Squeeze）。
"""

import pandas as pd
import numpy as np


def score_volatility(close: pd.Series) -> dict:
    """
    評估波動率壓縮/擴張狀態。

    Parameters
    ----------
    close : Series of daily close prices (sorted by date, >= 80 entries)

    Returns
    -------
    dict: {"score": float, "squeeze_ratio": float, "bb_position": str, "details": str}
    """
    result = {
        "score": 0,
        "squeeze_ratio": 0,
        "bb_position": "",
        "details": "",
    }

    if len(close) < 80:
        result["details"] = "數據不足"
        return result

    # 布林帶（20日）
    ma20 = close.rolling(20).mean()
    std20 = close.rolling(20).std()
    upper = ma20 + 2 * std20
    lower = ma20 - 2 * std20

    # 帶寬
    bb_width = (upper - lower) / ma20
    bb_width = bb_width.dropna()

    if len(bb_width) < 60:
        result["details"] = "布林帶數據不足"
        return result

    current_width = bb_width.iloc[-1]
    min_width_60d = bb_width.iloc[-60:].min()

    # Squeeze ratio
    squeeze_ratio = current_width / min_width_60d if min_width_60d > 0 else 1.0

    # 當前股價相對布林帶位置
    current_price = close.iloc[-1]
    current_upper = upper.iloc[-1]
    current_lower = lower.iloc[-1]
    current_ma = ma20.iloc[-1]

    if current_price > current_upper:
        bb_position = "above_upper"
    elif current_price > current_ma:
        bb_position = "upper_half"
    elif current_price > current_lower:
        bb_position = "lower_half"
    else:
        bb_position = "below_lower"

    # 帶寬在近60日的百分位
    width_percentile = (bb_width.iloc[-60:] < current_width).mean() * 100

    result["squeeze_ratio"] = round(squeeze_ratio, 2)
    result["bb_position"] = bb_position

    # 評分
    score = 0
    if squeeze_ratio > 2.0 and bb_position == "above_upper":
        score = 95  # 盤整後爆發，突破上軌
    elif squeeze_ratio > 2.0 and bb_position == "upper_half":
        score = 85  # 擴張中，在上半部
    elif squeeze_ratio > 1.5 and bb_position in ("above_upper", "upper_half"):
        score = 75
    elif squeeze_ratio > 1.5:
        score = 60
    elif width_percentile < 20:
        score = 50  # 正在壓縮中（醞釀期）
    elif width_percentile < 40:
        score = 40
    else:
        score = 25

    result["score"] = score
    result["details"] = (
        f"Squeeze:{squeeze_ratio:.1f}x "
        f"BB:{bb_position} "
        f"寬度%ile:{width_percentile:.0f}"
    )
    return result
