"""
scoring/institutional_flow.py — 維度 8：法人買賣超（台股特有）
追蹤三大法人（外資、投信、自營商）的連續買賣超行為。
外資+投信同步買超是最強的訊號。
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)


def score_institutional_flow(
    stock_id: str,
    inst_data: pd.DataFrame,
) -> dict:
    """
    評估法人買賣超動能。

    Parameters
    ----------
    stock_id : e.g. "2330"
    inst_data : DataFrame with columns [stock_id, date, foreign_net, trust_net, dealer_net]

    Returns
    -------
    dict: {
        "score": float (0-100),
        "foreign_consecutive": int,
        "trust_consecutive": int,
        "both_buying": bool,
        "details": str,
    }
    """
    result = {
        "score": 30,  # 預設中性
        "foreign_consecutive": 0,
        "trust_consecutive": 0,
        "both_buying": False,
        "details": "無法人資料",
    }

    if inst_data.empty or "stock_id" not in inst_data.columns:
        return result

    # 篩選該股票
    df = inst_data[inst_data["stock_id"] == stock_id].copy()
    if df.empty:
        return result

    df = df.sort_values("date", ascending=True).reset_index(drop=True)

    # 計算外資連續買超天數
    foreign_consec = _consecutive_positive(df["foreign_net"].values)

    # 計算投信連續買超天數
    trust_consec = _consecutive_positive(df["trust_net"].values)

    # 近 5 日外資淨買超合計
    recent_5 = df.tail(5)
    foreign_5d_net = recent_5["foreign_net"].sum() if len(recent_5) > 0 else 0
    trust_5d_net = recent_5["trust_net"].sum() if len(recent_5) > 0 else 0

    # 外資+投信是否同步買超（近 3 日都是）
    recent_3 = df.tail(3)
    both_buying = False
    if len(recent_3) >= 3:
        both_buying = (
            (recent_3["foreign_net"] > 0).all()
            and (recent_3["trust_net"] > 0).all()
        )

    result["foreign_consecutive"] = foreign_consec
    result["trust_consecutive"] = trust_consec
    result["both_buying"] = both_buying

    # ── 評分 ──
    score = 30  # 基線

    # 外資連續買超加分
    if foreign_consec >= 10:
        score += 25
    elif foreign_consec >= 5:
        score += 15
    elif foreign_consec >= 3:
        score += 8

    # 投信連續買超加分
    if trust_consec >= 10:
        score += 25
    elif trust_consec >= 5:
        score += 15
    elif trust_consec >= 3:
        score += 8

    # 外資+投信同步買超（最強訊號）
    if both_buying:
        score += 15

    # 外資連續賣超扣分
    if foreign_consec < 0 and abs(foreign_consec) >= 5:
        score -= 15
    if trust_consec < 0 and abs(trust_consec) >= 5:
        score -= 10

    score = max(0, min(100, score))
    result["score"] = score

    # 格式化細節
    f_dir = f"外資連買{foreign_consec}日" if foreign_consec > 0 else (
        f"外資連賣{abs(foreign_consec)}日" if foreign_consec < 0 else "外資中立"
    )
    t_dir = f"投信連買{trust_consec}日" if trust_consec > 0 else (
        f"投信連賣{abs(trust_consec)}日" if trust_consec < 0 else "投信中立"
    )
    sync = " ⭐同步買超" if both_buying else ""

    result["details"] = f"{f_dir} {t_dir}{sync}"
    return result


def _consecutive_positive(values) -> int:
    """
    計算末尾連續正數（買超）或負數（賣超）的天數。
    正數 = 連續買超天數（正值）
    負數 = 連續賣超天數（負值）
    """
    if len(values) == 0:
        return 0

    count = 0
    last_val = values[-1]

    if last_val > 0:
        for v in reversed(values):
            if v > 0:
                count += 1
            else:
                break
        return count
    elif last_val < 0:
        for v in reversed(values):
            if v < 0:
                count += 1
            else:
                break
        return -count
    else:
        return 0
