"""
scoring/volume_analysis.py — 維度 3：成交量異常
衡量近期成交量是否異常放大，以及量價配合度。
"""

import pandas as pd
import numpy as np


def score_volume(close: pd.Series, volume: pd.Series) -> dict:
    """
    評估成交量異常程度與量價配合度。

    Parameters
    ----------
    close : Series of daily close prices (sorted by date)
    volume : Series of daily volume (aligned with close)

    Returns
    -------
    dict: {"score": float, "vol_ratio_5d": float, "up_down_vol_ratio": float, "details": str}
    """
    result = {
        "score": 0,
        "vol_ratio_5d": 0,
        "vol_ratio_20d": 0,
        "up_down_vol_ratio": 0,
        "details": "",
    }

    if len(close) < 61 or len(volume) < 61:
        result["details"] = "數據不足"
        return result

    # 過濾零成交量
    vol = volume.copy()
    vol = vol.replace(0, np.nan)

    # 成交量比率
    avg_vol_60d = vol.iloc[-60:].mean()
    if pd.isna(avg_vol_60d) or avg_vol_60d <= 0:
        result["details"] = "成交量數據異常"
        return result

    avg_vol_5d = vol.iloc[-5:].mean()
    avg_vol_20d = vol.iloc[-20:].mean()

    vol_ratio_5d = avg_vol_5d / avg_vol_60d if avg_vol_60d > 0 else 0
    vol_ratio_20d = avg_vol_20d / avg_vol_60d if avg_vol_60d > 0 else 0

    # 上漲日/下跌日量能比（近 20 日）
    recent_close = close.iloc[-21:]
    recent_vol = vol.iloc[-21:]
    daily_returns = recent_close.pct_change()

    up_days = daily_returns > 0
    down_days = daily_returns < 0

    up_vol = recent_vol[up_days.values].mean() if up_days.sum() > 0 else 0
    down_vol = recent_vol[down_days.values].mean() if down_days.sum() > 0 else 0

    up_down_ratio = up_vol / down_vol if down_vol > 0 else 2.0

    result["vol_ratio_5d"] = round(vol_ratio_5d, 2)
    result["vol_ratio_20d"] = round(vol_ratio_20d, 2)
    result["up_down_vol_ratio"] = round(up_down_ratio, 2)

    # 評分
    score = 0
    if vol_ratio_5d > 2.0 and up_down_ratio > 1.5:
        score = 95
    elif vol_ratio_5d > 2.0 and up_down_ratio > 1.0:
        score = 85
    elif vol_ratio_5d > 1.5 and up_down_ratio > 1.2:
        score = 75
    elif vol_ratio_5d > 1.5:
        score = 65
    elif vol_ratio_20d > 1.2 and up_down_ratio > 1.0:
        score = 55
    elif vol_ratio_20d > 1.0:
        score = 40
    else:
        score = 20

    result["score"] = score
    result["details"] = (
        f"5d量比:{vol_ratio_5d:.1f}x "
        f"漲跌量比:{up_down_ratio:.1f}"
    )
    return result
