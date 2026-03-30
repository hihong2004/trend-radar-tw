"""
scoring/relative_strength.py — 維度 1：相對強度
衡量個股相對於大盤 (SPY) 的超額報酬表現。
"""

import pandas as pd
import numpy as np


def compute_returns(close: pd.Series, periods: int) -> float:
    """計算 N 日報酬率"""
    if len(close) < periods + 1:
        return np.nan
    return (close.iloc[-1] / close.iloc[-1 - periods]) - 1


def score_relative_strength(
    stock_close: pd.Series,
    benchmark_close: pd.Series,
    all_rs_scores: pd.Series = None,
) -> dict:
    """
    計算相對強度評分。

    Parameters
    ----------
    stock_close : Series of daily close prices (sorted by date)
    benchmark_close : Series of SPY close prices (aligned dates)
    all_rs_scores : Series of all stocks' raw RS scores (for percentile ranking)

    Returns
    -------
    dict: {
        "score": float (0-100),
        "rs_20d": float,
        "rs_60d": float,
        "raw_rs": float,
        "details": str,
    }
    """
    result = {"score": 0, "rs_20d": 0, "rs_60d": 0, "raw_rs": 0, "details": ""}

    # 計算超額報酬
    stock_ret_20 = compute_returns(stock_close, 20)
    bench_ret_20 = compute_returns(benchmark_close, 20)
    stock_ret_60 = compute_returns(stock_close, 60)
    bench_ret_60 = compute_returns(benchmark_close, 60)

    if any(np.isnan(x) for x in [stock_ret_20, bench_ret_20, stock_ret_60, bench_ret_60]):
        result["details"] = "數據不足"
        return result

    rs_20d = stock_ret_20 - bench_ret_20
    rs_60d = stock_ret_60 - bench_ret_60

    # 加權合成原始 RS 分數
    raw_rs = rs_20d * 0.4 + rs_60d * 0.6

    result["rs_20d"] = round(rs_20d * 100, 2)
    result["rs_60d"] = round(rs_60d * 100, 2)
    result["raw_rs"] = round(raw_rs * 100, 2)

    # 如果有全體排名數據，計算百分位
    if all_rs_scores is not None and len(all_rs_scores) > 10:
        percentile = (all_rs_scores < raw_rs).mean() * 100
        result["score"] = round(min(percentile, 100), 1)
    else:
        # 無全體數據時，用絕對值估算
        if raw_rs > 0.15:
            result["score"] = 90
        elif raw_rs > 0.08:
            result["score"] = 75
        elif raw_rs > 0.03:
            result["score"] = 60
        elif raw_rs > 0:
            result["score"] = 45
        else:
            result["score"] = max(0, 30 + raw_rs * 200)

    result["details"] = f"RS20d:{rs_20d*100:+.1f}% RS60d:{rs_60d*100:+.1f}%"
    return result


def compute_all_raw_rs(ohlcv: pd.DataFrame, benchmark_close: pd.Series) -> pd.Series:
    """
    計算所有 ticker 的原始 RS 分數（用於全體排名）。
    Returns: Series indexed by ticker, values = raw RS score
    """
    results = {}
    for ticker, group in ohlcv.groupby("ticker"):
        close = group.sort_values("date")["close"]
        if len(close) < 61:
            continue

        stock_ret_20 = compute_returns(close, 20)
        bench_ret_20 = compute_returns(benchmark_close, 20)
        stock_ret_60 = compute_returns(close, 60)
        bench_ret_60 = compute_returns(benchmark_close, 60)

        if any(np.isnan(x) for x in [stock_ret_20, bench_ret_20, stock_ret_60, bench_ret_60]):
            continue

        rs_20d = stock_ret_20 - bench_ret_20
        rs_60d = stock_ret_60 - bench_ret_60
        results[ticker] = rs_20d * 0.4 + rs_60d * 0.6

    return pd.Series(results)
