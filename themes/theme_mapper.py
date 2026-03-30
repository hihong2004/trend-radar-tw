"""
themes/theme_mapper.py — 主題→台股企業映射
Claude API 中文 prompt，映射到台灣上市櫃成分股。
"""

import json
import logging
import os
from datetime import datetime

import config

logger = logging.getLogger(__name__)

THEME_CACHE_PATH = os.path.join(os.path.dirname(__file__), "theme_cache_tw.json")
THEME_GROUPS_PATH = os.path.join(os.path.dirname(__file__), "theme_groups.json")


def _load_cache():
    if os.path.exists(THEME_CACHE_PATH):
        try:
            with open(THEME_CACHE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_cache(cache):
    try:
        with open(THEME_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"快取失敗: {e}")


def _is_cache_valid(entry):
    cached_date = entry.get("cached_date", "")
    if not cached_date:
        return False
    try:
        dt = datetime.strptime(cached_date, "%Y-%m-%d")
        return (datetime.now() - dt).days < config.THEME_CACHE_DAYS
    except Exception:
        return False


def map_theme_via_claude(keyword, category, acceleration):
    if not config.ANTHROPIC_API_KEY:
        logger.warning("ANTHROPIC_API_KEY 未設定")
        return _fallback_mapping(keyword, category)

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)

        system_prompt = (
            "你是一位台灣股市產業分析師。用戶會給你一個正在升溫的產業主題關鍵字。"
            "請從台灣上市櫃股票中，找出最直接受惠的企業。"
            "只回傳 JSON，不要任何其他文字或 markdown。"
            "格式：{\"theme\": \"主題名稱\", \"tickers\": [\"2330\", \"2454\"], "
            "\"reasoning\": \"簡述為什麼這些企業受惠\"}"
            "\n只列出 4 碼股票代號（不含 .TW/.TWO），5-15 檔，不要硬湊。"
        )

        user_prompt = (
            f"正在升溫的台股主題：{keyword}\n"
            f"分類：{category}\n"
            f"Google Trends 加速度：{acceleration:.1f}x\n"
            f"請映射到台灣上市櫃的關聯企業股票代號。"
        )

        response = client.messages.create(
            model=config.CLAUDE_MODEL,
            max_tokens=config.CLAUDE_MAX_TOKENS,
            messages=[{"role": "user", "content": user_prompt}],
            system=system_prompt,
        )

        text = response.content[0].text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

        result = json.loads(text)
        result["source"] = "claude_api"
        result["cached_date"] = datetime.now().strftime("%Y-%m-%d")
        result["keyword"] = keyword
        result["category"] = category

        logger.info(f"  🤖 Claude: {keyword} → {result.get('tickers', [])}")
        return result

    except Exception as e:
        logger.error(f"  Claude API 失敗: {e}")
        return _fallback_mapping(keyword, category)


def _fallback_mapping(keyword, category):
    fallback = {
        "AI半導體": ["2330", "2454", "3034", "2379", "3711", "6488"],
        "散熱": ["3017", "6117", "2059", "3653", "6414"],
        "矽光子": ["4960", "2379", "3714", "3443"],
        "重電": ["1503", "1504", "1513", "1514", "8261"],
        "軍工國防": ["2634", "2208", "4535", "2233"],
        "生技醫療": ["6547", "4743", "1760", "4174"],
        "記憶體": ["8271", "4967", "3006", "2337"],
        "綠能": ["6244", "3576", "6491"],
        "航運": ["2603", "2609", "2615"],
        "電動車": ["2201", "3702", "6488", "2308"],
        "ABF載板": ["2313", "3037", "8046"],
        "IP矽智財": ["3443", "2454", "3661"],
        "營建資產": ["2504", "2542", "2520"],
        "觀光餐飲": ["2706", "2707", "2727"],
        "機器人": ["2317", "4506", "2308"],
        "低軌衛星": ["3372", "6285", "3363"],
        "銅箔基板": ["2313", "3037", "8046"],
        "高股息ETF": ["0056", "00878", "00919"],
    }

    tickers = fallback.get(category, [])
    return {
        "theme": keyword,
        "tickers": tickers,
        "reasoning": f"備用映射（{category}）",
        "source": "fallback",
        "cached_date": datetime.now().strftime("%Y-%m-%d"),
        "keyword": keyword,
        "category": category,
    }


def map_rising_themes(rising_themes):
    cache = _load_cache()
    mappings = []

    for theme in rising_themes:
        keyword = theme["keyword"]
        category = theme.get("category", "")
        acceleration = theme.get("acceleration", 0)

        cache_key = keyword.strip()
        if cache_key in cache and _is_cache_valid(cache[cache_key]):
            logger.info(f"  📦 快取: {keyword}")
            mapping = cache[cache_key]
            mapping["acceleration"] = acceleration
            mappings.append(mapping)
            continue

        logger.info(f"  🔄 映射: {keyword}")
        mapping = map_theme_via_claude(keyword, category, acceleration)
        mapping["acceleration"] = acceleration
        cache[cache_key] = mapping
        mappings.append(mapping)

    _save_cache(cache)
    return mappings


def update_theme_groups(mappings):
    existing = {"active_themes": [], "cooling_themes": []}
    if os.path.exists(THEME_GROUPS_PATH):
        try:
            with open(THEME_GROUPS_PATH, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            pass

    active_themes = []
    seen = set()

    for m in mappings:
        keyword = m.get("keyword", m.get("theme", ""))
        key = keyword.strip()
        if key in seen:
            continue
        seen.add(key)

        active_themes.append({
            "theme": m.get("theme", keyword),
            "keyword": keyword,
            "category": m.get("category", ""),
            "acceleration": m.get("acceleration", 0),
            "status": "rising" if m.get("acceleration", 0) >= config.TRENDS_ACCELERATION_THRESHOLD else "stable",
            "tickers": m.get("tickers", []),
            "reasoning": m.get("reasoning", ""),
            "source": m.get("source", ""),
            "first_detected": m.get("cached_date", datetime.now().strftime("%Y-%m-%d")),
        })

    cooling = []
    for old in existing.get("active_themes", []):
        old_key = old.get("keyword", old.get("theme", "")).strip()
        if old_key not in seen:
            old["status"] = "cooling"
            old["acceleration"] = old.get("acceleration", 1.0) * 0.8
            cooling.append(old)

    cooling = [t for t in cooling if t.get("acceleration", 0) > 0.3][:20]

    output = {
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "active_themes": active_themes,
        "cooling_themes": cooling,
    }

    try:
        with open(THEME_GROUPS_PATH, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ 主題群組: {len(active_themes)} 活躍, {len(cooling)} 降溫")
    except Exception as e:
        logger.error(f"儲存失敗: {e}")

    return output


def run_theme_discovery():
    from themes.trend_scanner import scan_all_themes

    logger.info("🌐 台股主題自動發現...")

    scan_result = scan_all_themes()
    rising = scan_result.get("rising_themes", [])

    if not rising:
        logger.info("📭 無升溫主題")
        update_theme_groups([])
        return scan_result

    logger.info(f"🔥 {len(rising)} 個升溫主題")
    mappings = map_rising_themes(rising)
    groups = update_theme_groups(mappings)

    scan_result["mappings"] = mappings
    scan_result["theme_groups"] = groups

    return scan_result
