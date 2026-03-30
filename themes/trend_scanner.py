"""
themes/trend_scanner.py — Google Trends 掃描器（台灣版）
掃描中文種子關鍵字，偵測升溫主題。geo='TW'。
"""

import json
import time
import logging
import os
from datetime import datetime

import pandas as pd
import config

logger = logging.getLogger(__name__)

SEEDS_PATH = os.path.join(os.path.dirname(__file__), "keyword_seeds_tw.json")
SCAN_RESULTS_PATH = os.path.join(config.CACHE_DIR, "trends_scan_tw.json")


def load_seed_keywords() -> dict:
    with open(SEEDS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def scan_single_keyword(pytrends_client, keyword: str) -> dict:
    result = {
        "keyword": keyword,
        "acceleration": 0,
        "recent_avg": 0,
        "baseline_avg": 0,
        "current_value": 0,
        "related_queries": [],
        "status": "error",
    }

    try:
        pytrends_client.build_payload(
            [keyword],
            timeframe=config.TRENDS_TIMEFRAME,
            geo=config.TRENDS_GEO,
        )

        interest = pytrends_client.interest_over_time()

        if interest.empty or keyword not in interest.columns:
            result["status"] = "no_data"
            return result

        values = interest[keyword].values

        if len(values) < 8:
            result["status"] = "insufficient_data"
            return result

        recent = values[-2:]
        baseline = values[-8:-2]

        recent_avg = float(recent.mean())
        baseline_avg = float(baseline.mean())

        if baseline_avg > 0:
            acceleration = recent_avg / baseline_avg
        elif recent_avg > 0:
            acceleration = 3.0
        else:
            acceleration = 0

        result["recent_avg"] = round(recent_avg, 1)
        result["baseline_avg"] = round(baseline_avg, 1)
        result["acceleration"] = round(acceleration, 2)
        result["current_value"] = int(values[-1])

        if acceleration >= config.TRENDS_ACCELERATION_THRESHOLD:
            result["status"] = "rising"
        elif acceleration >= 0.8:
            result["status"] = "stable"
        else:
            result["status"] = "declining"

        try:
            related = pytrends_client.related_queries()
            if keyword in related and related[keyword].get("rising") is not None:
                rising_df = related[keyword]["rising"]
                if not rising_df.empty and "query" in rising_df.columns:
                    result["related_queries"] = rising_df["query"].head(5).tolist()
        except Exception:
            pass

    except Exception as e:
        logger.warning(f"  ⚠️ '{keyword}' 失敗: {e}")
        result["status"] = "error"

    return result


def scan_all_themes() -> dict:
    from pytrends.request import TrendReq

    logger.info("🌐 Google Trends 台灣掃描...")

    pytrends = TrendReq(hl="zh-TW", tz=-480, timeout=(10, 30))
    seeds = load_seed_keywords()

    all_results = []
    rising_themes = []
    new_discoveries = []
    total = sum(len(v) for v in seeds.values())
    scanned = 0

    for category, keywords in seeds.items():
        logger.info(f"  📂 {category} ({len(keywords)} 個)")

        for keyword in keywords:
            scanned += 1
            logger.info(f"    [{scanned}/{total}] {keyword}")

            result = scan_single_keyword(pytrends, keyword)
            result["category"] = category
            all_results.append(result)

            if result["status"] == "rising":
                rising_themes.append(result)
                logger.info(f"    🔺 升溫! {result['acceleration']:.1f}x")

                for rq in result.get("related_queries", []):
                    all_seeds_flat = [
                        kw for kws in seeds.values() for kw in kws
                    ]
                    if rq not in all_seeds_flat:
                        new_discoveries.append(rq)

            time.sleep(config.TRENDS_QUERY_DELAY)

    new_discoveries = list(set(new_discoveries))[:20]

    scan_result = {
        "scan_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total_scanned": scanned,
        "rising_count": len(rising_themes),
        "rising_themes": rising_themes,
        "all_results": all_results,
        "new_discoveries": new_discoveries,
    }

    try:
        with open(SCAN_RESULTS_PATH, "w", encoding="utf-8") as f:
            json.dump(scan_result, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"儲存失敗: {e}")

    logger.info(f"✅ 掃描完成: {scanned} 關鍵字, {len(rising_themes)} 升溫, {len(new_discoveries)} 新發現")
    return scan_result


def get_rising_themes() -> list:
    if not os.path.exists(SCAN_RESULTS_PATH):
        return []
    try:
        mod_time = datetime.fromtimestamp(os.path.getmtime(SCAN_RESULTS_PATH))
        if (datetime.now() - mod_time).days > 10:
            return []
        with open(SCAN_RESULTS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("rising_themes", [])
    except Exception:
        return []
