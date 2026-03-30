"""
scoring/sector_momentum.py — 維度 5：產業族群共振（台股版）
雙層計算：證交所產業分類 + 概念股群組，取較高分。
"""

import pandas as pd
import numpy as np
import json
import os
import logging

import config

logger = logging.getLogger(__name__)

CONCEPT_GROUPS_PATH = os.path.join(config.BASE_DIR, "concept_groups.json")


def _load_concept_groups() -> dict:
    """載入概念股群組"""
    if os.path.exists(CONCEPT_GROUPS_PATH):
        try:
            with open(CONCEPT_GROUPS_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _ticker_to_stockid(ticker: str) -> str:
    """從 ticker (e.g. '2330.TW') 提取 stock_id (e.g. '2330')"""
    return ticker.split(".")[0]


def score_sector_momentum(
    ticker: str,
    sector: str,
    all_rs_scores: pd.Series,
    sector_map: dict,
    sector_etf_rs: dict = None,
) -> dict:
    """
    雙層族群共振評分。
    Layer 1: 證交所產業分類
    Layer 2: 概念股群組
    取兩者較高分。
    """
    result = {
        "score": 0,
        "sector_strong_pct": 0,
        "concept_strong_pct": 0,
        "concept_group": "",
        "details": "",
    }

    stock_id = _ticker_to_stockid(ticker)

    # ── Layer 1: 官方產業分類 ──
    layer1_score = 0
    sector_pct = 0

    if sector and sector != "":
        peers = [t for t, s in sector_map.items() if s == sector and t != ticker]
        if len(peers) >= 3 and len(all_rs_scores) > 10:
            rs_70th = all_rs_scores.quantile(0.70)
            peer_rs = all_rs_scores.reindex(peers).dropna()
            if len(peer_rs) > 0:
                sector_pct = (peer_rs > rs_70th).mean() * 100

            if sector_pct > 50:
                layer1_score = 90
            elif sector_pct > 30:
                layer1_score = 70
            elif sector_pct > 15:
                layer1_score = 50
            else:
                layer1_score = 25

    result["sector_strong_pct"] = round(sector_pct, 1)

    # ── Layer 2: 概念股群組 ──
    layer2_score = 0
    best_concept = ""
    concept_pct = 0

    concept_groups = _load_concept_groups()

    for group_name, members in concept_groups.items():
        if stock_id not in members:
            continue

        # 找同群組的其他成員
        peer_tickers = []
        for sid in members:
            if sid == stock_id:
                continue
            for suffix in [config.TWSE_SUFFIX, config.TPEX_SUFFIX]:
                t = sid + suffix
                if t in all_rs_scores.index:
                    peer_tickers.append(t)
                    break

        if len(peer_tickers) < 2:
            continue

        if len(all_rs_scores) > 10:
            rs_70th = all_rs_scores.quantile(0.70)
            peer_rs = all_rs_scores.reindex(peer_tickers).dropna()
            if len(peer_rs) > 0:
                pct = (peer_rs > rs_70th).mean() * 100
                if pct > concept_pct:
                    concept_pct = pct
                    best_concept = group_name

    if concept_pct > 50:
        layer2_score = 95
    elif concept_pct > 30:
        layer2_score = 75
    elif concept_pct > 15:
        layer2_score = 55
    else:
        layer2_score = 25

    result["concept_strong_pct"] = round(concept_pct, 1)
    result["concept_group"] = best_concept

    # ── 取較高分 ──
    score = max(layer1_score, layer2_score)
    if score == 0:
        score = 30  # 中性

    result["score"] = score

    details_parts = []
    if sector:
        details_parts.append(f"{sector}:{sector_pct:.0f}%")
    if best_concept:
        details_parts.append(f"{best_concept}:{concept_pct:.0f}%")
    result["details"] = " | ".join(details_parts) if details_parts else "無分類"

    return result


def compute_sector_etf_rs(sector_etf_ohlcv, benchmark_close):
    """台股版不使用產業 ETF，回傳空 dict"""
    return {}


def map_sector_to_etf_rs(sector, etf_percentiles):
    """台股版不使用，回傳中性值"""
    return 50
