"""
yahoo_classifier.py — Yahoo 股市分類爬蟲
從 tw.stock.yahoo.com 爬取：
  1. 概念股群組（~100+ 個，含成分股代號）
  2. 電子產業細分（~30 個子分類）
  3. 集團股（~50 個集團）

用於取代手動維護的 concept_groups.json。
"""

import logging
import requests
import pandas as pd
import json
import os
import re
import time
from datetime import datetime
from bs4 import BeautifulSoup

import config

logger = logging.getLogger(__name__)

YAHOO_CLASS_URL = "https://tw.stock.yahoo.com/class/"
YAHOO_CLASS_DETAIL_URL = "https://tw.stock.yahoo.com/class/{class_id}"

YAHOO_CACHE = os.path.join(config.CACHE_DIR, "yahoo_classifications.json")

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}


def fetch_class_index() -> dict:
    """
    爬取 Yahoo 股市分類首頁，取得所有分類連結。

    Returns
    -------
    dict: {
        "concepts": [{"name": str, "url": str}, ...],
        "electronics": [...],
        "conglomerates": [...],
    }
    """
    logger.info("🌐 爬取 Yahoo 股市分類首頁...")

    try:
        resp = requests.get(YAHOO_CLASS_URL, headers=_HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        categories = {
            "concepts": [],
            "electronics": [],
            "conglomerates": [],
        }

        # 找所有分類區塊
        # Yahoo 的結構是多個區塊，每個有標題和連結列表
        sections = soup.find_all("div", class_=re.compile(r"class|category|section", re.I))

        # 備用方式：直接找所有連結
        all_links = soup.find_all("a", href=True)

        for link in all_links:
            href = link.get("href", "")
            text = link.get_text(strip=True)

            if not text or len(text) > 20:
                continue

            # 概念股連結格式: /class/概念股名稱
            if "/class/" in href and text:
                # 嘗試從上下文判斷分類類型
                entry = {"name": text, "url": href}

                # 根據 URL 或文字特徵分類
                if any(kw in href for kw in ["concept", "theme"]):
                    categories["concepts"].append(entry)
                elif any(kw in href for kw in ["group", "conglomerate"]):
                    categories["conglomerates"].append(entry)
                else:
                    # 預設歸入概念股
                    categories["concepts"].append(entry)

        # 去重
        for key in categories:
            seen = set()
            deduped = []
            for item in categories[key]:
                if item["name"] not in seen:
                    seen.add(item["name"])
                    deduped.append(item)
            categories[key] = deduped

        total = sum(len(v) for v in categories.values())
        logger.info(f"  找到 {total} 個分類連結")

        return categories

    except Exception as e:
        logger.error(f"Yahoo 分類首頁爬取失敗: {e}")
        return {"concepts": [], "electronics": [], "conglomerates": []}


def fetch_class_members(class_name: str) -> list:
    """
    爬取某個分類下的成分股代號。

    Parameters
    ----------
    class_name : 分類名稱（用於 URL）

    Returns
    -------
    list of stock_id strings, e.g. ["2330", "2454", ...]
    """
    url = f"https://tw.stock.yahoo.com/class/{class_name}/"

    try:
        resp = requests.get(url, headers=_HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        stock_ids = set()

        # 方法 1: 找所有像股票代號的連結（4位數字）
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            text = link.get_text(strip=True)

            # 股票頁面連結格式: /quote/2330.TW 或 /quote/2330
            match = re.search(r"/quote/(\d{4})", href)
            if match:
                stock_ids.add(match.group(1))
                continue

            # 文字本身是 4 位數字
            if re.match(r"^\d{4}$", text):
                stock_ids.add(text)

        # 方法 2: 找表格中的股票代號
        for td in soup.find_all("td"):
            text = td.get_text(strip=True)
            if re.match(r"^\d{4}$", text):
                stock_ids.add(text)

        # 方法 3: 找 span 中的股票代號
        for span in soup.find_all("span"):
            text = span.get_text(strip=True)
            if re.match(r"^\d{4}$", text):
                stock_ids.add(text)

        return sorted(list(stock_ids))

    except Exception as e:
        logger.debug(f"  {class_name} 成分股爬取失敗: {e}")
        return []


def scrape_all_classifications(max_per_category: int = 150) -> dict:
    """
    完整爬取所有分類及其成分股。

    Returns
    -------
    dict: {
        "scraped_date": str,
        "groups": {
            "分類名稱": ["2330", "2454", ...],
            ...
        },
        "stats": {
            "total_groups": int,
            "total_stocks": int,
        }
    }
    """
    logger.info("=" * 50)
    logger.info("🌐 開始爬取 Yahoo 股市分類...")
    logger.info("=" * 50)

    # 取得分類首頁的所有連結
    index = fetch_class_index()

    all_class_names = set()
    for cat_list in index.values():
        for item in cat_list:
            all_class_names.add(item["name"])

    # 如果從首頁解析失敗，使用預設的熱門概念股清單
    if len(all_class_names) < 10:
        logger.warning("⚠️ 首頁解析結果太少，使用預設概念股清單")
        all_class_names = _get_fallback_class_names()

    logger.info(f"📂 共 {len(all_class_names)} 個分類待爬取")

    groups = {}
    scanned = 0

    for name in sorted(all_class_names):
        if scanned >= max_per_category:
            break

        scanned += 1
        logger.info(f"  [{scanned}/{len(all_class_names)}] {name}")

        members = fetch_class_members(name)

        if members and len(members) >= 2:
            groups[name] = members
            logger.info(f"    ✅ {len(members)} 檔成分股")
        else:
            logger.debug(f"    ⬜ 無成分股或太少")

        time.sleep(1)  # 控制爬取頻率

    # 統計
    total_stocks = len(set(sid for sids in groups.values() for sid in sids))

    result = {
        "scraped_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "groups": groups,
        "stats": {
            "total_groups": len(groups),
            "total_stocks": total_stocks,
        },
    }

    logger.info(f"✅ 爬取完成: {len(groups)} 個群組, {total_stocks} 檔不重複股票")

    return result


def _get_fallback_class_names() -> set:
    """預設的台股概念股 / 產業細分名稱"""
    return {
        # 熱門概念股
        "AI人工智慧", "台積電", "聯發科", "鴻海", "台達電",
        "蘋果200大供應商", "MOSFET", "AI理財機器人",
        "汽車電子", "5G", "半導體設備", "矽智財(IP)",
        "衛星/低軌衛星", "機器人/智慧機械", "電動車/油電車",
        "雲端產業", "Mini LED", "FinTech",
        "國防自主", "功率半導體", "穿戴裝置",
        "比特幣挖礦", "航空/航太", "智慧醫療",
        "人臉辨識", "物聯網", "工業4.0",
        "再生循環", "Tesla", "Apple watch",
        # 電子細分
        "IC生產製造", "IC設計服務", "其他光電",
        "組裝代工", "軟體設計", "系統整合",
        "PCB", "機殼", "電子連接相關",
        "光電設備", "網通設備組件", "被動元件",
        "電池或電源", "面板業", "LED",
        "設備或廠務工程", "主機板", "手機相關",
        # 集團
        "台塑", "鴻海", "中信", "富邦", "國泰",
        "台積電", "聯華", "遠東/亞東", "長榮",
        "華碩", "和碩", "廣達", "緯創",
    }


def save_yahoo_classifications(result: dict):
    """儲存爬取結果到快取"""
    try:
        with open(YAHOO_CACHE, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        logger.info(f"💾 Yahoo 分類已快取")
    except Exception as e:
        logger.warning(f"快取儲存失敗: {e}")


def load_yahoo_classifications() -> dict:
    """載入快取的 Yahoo 分類"""
    if os.path.exists(YAHOO_CACHE):
        try:
            with open(YAHOO_CACHE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"groups": {}}


def update_concept_groups_from_yahoo():
    """
    從 Yahoo 爬取分類並更新 concept_groups.json。
    保留原有的手動分類，加上 Yahoo 的分類。
    """
    import json

    concept_path = os.path.join(config.BASE_DIR, "concept_groups.json")

    # 載入現有手動分類
    existing = {}
    if os.path.exists(concept_path):
        try:
            with open(concept_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except Exception:
            pass

    # 爬取 Yahoo 分類
    result = scrape_all_classifications()
    save_yahoo_classifications(result)

    yahoo_groups = result.get("groups", {})

    # 合併：Yahoo 分類 + 手動分類（手動優先）
    merged = {}

    # 先加入 Yahoo 的
    for name, members in yahoo_groups.items():
        merged[f"[Yahoo]{name}"] = members

    # 再加入手動的（不覆蓋）
    for name, members in existing.items():
        if name not in merged:
            merged[name] = members

    # 儲存合併結果
    try:
        with open(concept_path, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ concept_groups.json 已更新: {len(merged)} 個群組")
    except Exception as e:
        logger.error(f"concept_groups.json 更新失敗: {e}")

    return merged
