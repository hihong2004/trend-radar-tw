"""
scoring/theme_momentum.py — 維度 7：主題熱度
衡量該股是否屬於正在升溫的投資主題。
完整的 Google Trends + Claude API 邏輯在 themes/ 模組中實作。
此模組負責將主題數據轉換為評分。
"""

import json
import os
import logging

import config

logger = logging.getLogger(__name__)

THEME_GROUPS_PATH = os.path.join(config.BASE_DIR, "themes", "theme_groups.json")


def load_active_themes() -> list:
    """載入當前活躍的主題群組"""
    if not os.path.exists(THEME_GROUPS_PATH):
        return []
    try:
        with open(THEME_GROUPS_PATH, "r") as f:
            data = json.load(f)
        return data.get("active_themes", [])
    except Exception as e:
        logger.warning(f"載入主題群組失敗: {e}")
        return []


def score_theme_momentum(ticker: str) -> dict:
    """
    評估該股的主題熱度。

    Parameters
    ----------
    ticker : 股票代號

    Returns
    -------
    dict: {"score": float, "themes": list, "details": str}
    """
    result = {
        "score": 30,  # 預設中性分（不懲罰無主題的股票）
        "themes": [],
        "details": "無相關主題",
    }

    active_themes = load_active_themes()
    if not active_themes:
        result["details"] = "主題引擎尚未初始化"
        return result

    # 找出該 ticker 屬於哪些主題
    matched_themes = []
    max_acceleration = 0

    for theme in active_themes:
        if ticker in theme.get("tickers", []):
            matched_themes.append(theme)
            acc = theme.get("acceleration", 0)
            if acc > max_acceleration:
                max_acceleration = acc

    if not matched_themes:
        return result

    result["themes"] = [t["theme"] for t in matched_themes]

    # 評分：取最高加速度的主題
    if max_acceleration > 2.0:
        score = 95
    elif max_acceleration > 1.5:
        score = 80
    elif max_acceleration > 1.0:
        score = 60
    else:
        score = 45

    result["score"] = score
    result["details"] = ", ".join(
        f"{t['theme']}({t.get('acceleration', 0):.1f}x)"
        for t in matched_themes
    )
    return result
