"""
alerts/formatter_tw.py — 台股版 LINE 訊息格式化（完整中文版）
所有標的顯示「代號 中文名稱」，族群附帶簡介。
"""

import pandas as pd
from datetime import datetime
import config


def _stars_str(n):
    return "★" * n + "☆" * (5 - n)


def _get_stock_label(r):
    """組合 '代號 名稱' 標籤"""
    sid = r.get("stock_id", "")
    name = r.get("name", "")
    if name:
        return f"{sid} {name}"
    return sid


def _get_stock_label_from_df(scored_df, ticker):
    """從 scored_df 中找到 ticker 的中文標籤"""
    row = scored_df[scored_df["ticker"] == ticker]
    if row.empty:
        return ticker
    r = row.iloc[0]
    return _get_stock_label(r)


# 產業族群簡介對照表
SECTOR_DESCRIPTIONS = {
    "半導體業": "晶圓代工、IC 設計、封測、設備",
    "電腦及週邊設備業": "伺服器、筆電、桌機、周邊",
    "光電業": "面板、LED、光通訊、太陽能",
    "通信網路業": "網通設備、基地台、光纖",
    "電子零組件業": "被動元件、連接器、PCB",
    "其他電子業": "EMS、電子代工、模組",
    "電機機械": "馬達、自動化設備、工具機",
    "電器電纜": "電纜、變壓器、配電設備",
    "鋼鐵工業": "鋼鐵、鑄造、金屬加工",
    "航運業": "貨櫃、散裝、航空",
    "金融保險業": "銀行、壽險、證券",
    "建材營造業": "營建、建材、水泥",
    "食品工業": "食品加工、飲料、餐飲",
    "紡織纖維": "紡織、成衣、機能布料",
    "生技醫療業": "新藥、醫材、CDMO",
    "化學工業": "石化、特化、塑膠",
    "觀光餐旅": "飯店、旅行社、餐飲",
    "汽車工業": "整車、零組件、車用電子",
    "油電燃氣業": "電力、天然氣、油品",
    "資訊服務業": "SI、軟體、雲端服務",
    "電子通路業": "電子零件通路、代理",
}

# 概念股群組簡介對照表
CONCEPT_DESCRIPTIONS = {
    "AI伺服器供應鏈": "受惠 AI 算力需求爆發，含伺服器組裝、零組件、散熱",
    "台積電供應鏈": "台積電上下游供應商，含設備、材料、封測",
    "散熱概念股": "AI 伺服器帶動液冷/氣冷散熱需求大增",
    "矽光子概念股": "AI 資料中心光通訊需求，800G/1.6T 光模組",
    "重電概念股": "台電強韌電網計畫，變壓器、電纜、配電設備",
    "軍工概念股": "國防自主政策，國機國造、潛艦國造",
    "記憶體概念股": "HBM/DRAM/NAND 漲價循環受惠股",
    "ABF載板概念股": "AI 晶片帶動高階 IC 載板需求",
    "IP矽智財": "IC 設計服務、矽智財授權",
    "電動車概念股": "EV 整車、三電系統、充電樁",
    "生技醫療": "新藥開發、細胞治療、CDMO 代工",
    "航運概念股": "貨櫃/散裝運價循環",
    "觀光餐飲": "入境旅客復甦、飯店住房率回升",
    "綠能概念股": "太陽能、風電、儲能",
    "營建資產": "房市交易、都更危老、營建股",
    "金融龍頭": "銀行、壽險、證券龍頭股",
    "5G通訊": "5G 基站、小基站、網通升級",
    "低軌衛星": "低軌衛星通訊地面設備",
}


def format_daily_report(scored_df, transitions, theme_info=None):
    today = datetime.now().strftime("%Y-%m-%d")
    lines = []

    lines.append("═" * 22)
    lines.append("📡 台股趨勢雷達日報")
    lines.append(f"📅 {today}")
    lines.append("═" * 22)

    if scored_df.empty:
        lines.append("\n⚠️ 今日無評分數據")
        return "\n".join(lines)

    # ── 新進觀察名單 ──
    new_s1 = transitions.get("new_stage1", [])
    if new_s1:
        lines.append(f"\n🔥 新進觀察名單 ({len(new_s1)} 檔)")
        for ticker in new_s1[:10]:
            row = scored_df[scored_df["ticker"] == ticker]
            if row.empty:
                continue
            r = row.iloc[0]
            label = _get_stock_label(r)
            sector = r.get("sector", "")
            concept = r.get("concept_group", "")
            inst = r.get("details", {}).get("institutional", "") if isinstance(r.get("details"), dict) else ""
            rev = r.get("details", {}).get("revenue", "") if isinstance(r.get("details"), dict) else ""

            lines.append(
                f"  {label} {_stars_str(r['stars'])} {r['total_score']:.0f}分"
            )
            # 產業 + 概念股
            tags = []
            if sector:
                tags.append(sector)
            if concept:
                tags.append(concept)
            if tags:
                lines.append(f"    📂 {' / '.join(tags)}")
            # 法人 + 營收
            info_parts = []
            if inst:
                info_parts.append(inst)
            if rev:
                info_parts.append(rev)
            if info_parts:
                lines.append(f"    📊 {' | '.join(info_parts)}")

    # ── Stage 2 加速 ──
    stage2 = scored_df[scored_df.get("stage", pd.Series()) == 2].head(10) if "stage" in scored_df.columns else pd.DataFrame()
    if not stage2.empty:
        lines.append(f"\n🚀 趨勢加速中 ({len(stage2)} 檔)")
        for _, r in stage2.iterrows():
            label = _get_stock_label(r)
            days = r.get("days_above_70", 0)
            sector = r.get("sector", "")
            concept = r.get("concept_group", "")
            tag = concept if concept else sector
            lines.append(
                f"  {label} {_stars_str(r['stars'])} {r['total_score']:.0f}分"
                f" | {tag} | 第{days}天"
            )

    # ── 新確認趨勢 ──
    new_s2 = transitions.get("new_stage2", [])
    if new_s2:
        lines.append(f"\n⭐ 新確認趨勢 (→Stage 2)")
        for ticker in new_s2[:5]:
            label = _get_stock_label_from_df(scored_df, ticker)
            lines.append(f"  {label}")

    # ── 趨勢疲軟 ──
    decay = transitions.get("decay_stage3", [])
    if decay:
        lines.append(f"\n⚠️ 趨勢疲軟 ({len(decay)} 檔)")
        for ticker in decay[:5]:
            row = scored_df[scored_df["ticker"] == ticker]
            if row.empty:
                continue
            r = row.iloc[0]
            label = _get_stock_label(r)
            rs_detail = r.get("details", {}).get("rs", "") if isinstance(r.get("details"), dict) else ""
            lines.append(f"  {label} {r['total_score']:.0f}分 | {rs_detail}")

    # ── 外資+投信同步買超 ──
    if "institutional" in scored_df.columns:
        both_buying = scored_df[
            scored_df["institutional"].apply(
                lambda x: x.get("both_buying", False) if isinstance(x, dict) else False
            )
        ]
        # 只顯示高分的
        both_buying = both_buying[both_buying["total_score"] >= 55].head(8)
        if not both_buying.empty:
            lines.append(f"\n⭐ 外資+投信同步買超（★★★以上）")
            for _, r in both_buying.iterrows():
                label = _get_stock_label(r)
                fc = r["institutional"].get("foreign_consec", 0)
                tc = r["institutional"].get("trust_consec", 0)
                lines.append(
                    f"  {label} {r['total_score']:.0f}分"
                    f" | 外資連買{fc}日 投信連買{tc}日"
                )

    # ── 升溫主題（含簡介）──
    if theme_info and theme_info.get("active_themes"):
        active = [t for t in theme_info["active_themes"] if t.get("status") == "rising"]
        if active:
            lines.append(f"\n🌐 升溫主題 ({len(active)} 個)")
            for t in active[:5]:
                theme_name = t.get("theme", t.get("keyword", "?"))
                acc = t.get("acceleration", 0)
                tickers = t.get("tickers", [])
                reasoning = t.get("reasoning", "")

                lines.append(f"\n  🔺 {theme_name}（熱度 {acc:.1f}x）")

                # 查找簡介
                desc = CONCEPT_DESCRIPTIONS.get(theme_name, reasoning)
                if desc:
                    lines.append(f"    💡 {desc[:40]}")

                # 關聯個股（附中文名）
                if tickers and not scored_df.empty:
                    stock_labels = []
                    for sid in tickers[:6]:
                        matched = scored_df[scored_df["stock_id"] == sid]
                        if not matched.empty:
                            name = matched.iloc[0].get("name", "")
                            stock_labels.append(f"{sid}{name}")
                        else:
                            stock_labels.append(sid)
                    lines.append(f"    → {', '.join(stock_labels)}")

    # ── 產業強度（含簡介）──
    if "sector" in scored_df.columns:
        sector_strength = (
            scored_df[scored_df["total_score"] >= 55]
            .groupby("sector").size()
            .sort_values(ascending=False).head(5)
        )
        if not sector_strength.empty:
            lines.append("\n🏭 產業強度 (★★★以上)")
            for sector, count in sector_strength.items():
                total_in = (scored_df["sector"] == sector).sum()
                pct = count / total_in * 100 if total_in > 0 else 0
                desc = SECTOR_DESCRIPTIONS.get(sector, "")
                sector_line = f"  {sector}: {count}檔 ({pct:.0f}%)"
                if desc:
                    sector_line += f"\n    💡 {desc}"
                lines.append(sector_line)

    # ── 概念股群組強度 ──
    if "concept_group" in scored_df.columns:
        concept_strength = (
            scored_df[
                (scored_df["total_score"] >= 55)
                & (scored_df["concept_group"] != "")
            ]
            .groupby("concept_group").size()
            .sort_values(ascending=False).head(5)
        )
        if not concept_strength.empty:
            lines.append("\n💡 概念股群組強度")
            for concept, count in concept_strength.items():
                desc = CONCEPT_DESCRIPTIONS.get(concept, "")
                line = f"  {concept}: {count}檔走強"
                if desc:
                    line += f"\n    {desc[:35]}"
                lines.append(line)

    # ── 統計 ──
    watchlist = (scored_df["stars"] >= 4).sum()
    s1 = (scored_df.get("stage", pd.Series()) == 1).sum() if "stage" in scored_df.columns else 0
    s2 = (scored_df.get("stage", pd.Series()) == 2).sum() if "stage" in scored_df.columns else 0

    lines.append(f"\n📊 觀察名單: {watchlist} 檔 (★★★★以上)")
    lines.append(f"   Stage 1 覺醒: {s1} | Stage 2 加速: {s2}")

    # ── Top 5（完整資訊）──
    top5 = scored_df.head(5)
    if not top5.empty:
        lines.append("\n🏆 今日 Top 5")
        for _, r in top5.iterrows():
            label = _get_stock_label(r)
            sector = r.get("sector", "")
            concept = r.get("concept_group", "")
            tag = concept if concept else sector
            rev = r.get("details", {}).get("revenue", "") if isinstance(r.get("details"), dict) else ""
            inst = r.get("details", {}).get("institutional", "") if isinstance(r.get("details"), dict) else ""

            lines.append(
                f"  {label} {r['total_score']:.1f}分 {_stars_str(r['stars'])}"
            )
            info = f"    {tag}"
            if rev:
                info += f" | {rev}"
            if inst:
                info += f" | {inst}"
            lines.append(info)

    lines.append(f"\n{'═' * 22}")
    lines.append("📡 Trend Radar TW v1.0")

    return "\n".join(lines)


def format_short_alert(scored_df, transitions):
    today = datetime.now().strftime("%Y-%m-%d")
    lines = [f"📡 台股趨勢雷達 {today}", ""]

    new_s1 = transitions.get("new_stage1", [])
    new_s2 = transitions.get("new_stage2", [])
    decay = transitions.get("decay_stage3", [])

    if not new_s1 and not new_s2 and not decay:
        watchlist = (scored_df["stars"] >= 4).sum() if not scored_df.empty else 0
        lines.append(f"📊 無重大變化 | 觀察名單(★★★★+): {watchlist}檔")
        if not scored_df.empty:
            top3 = scored_df.head(3)
            lines.append("\n🏆 Top 3:")
            for _, r in top3.iterrows():
                label = _get_stock_label(r)
                sector = r.get("sector", "")
                lines.append(f"  {label} {r['total_score']:.0f}分 | {sector}")
    else:
        if new_s1:
            lines.append(f"🔥 新進觀察 ({len(new_s1)} 檔):")
            for t in new_s1[:5]:
                label = _get_stock_label_from_df(scored_df, t)
                lines.append(f"  {label}")
        if new_s2:
            lines.append(f"⭐ 趨勢確認:")
            for t in new_s2[:5]:
                label = _get_stock_label_from_df(scored_df, t)
                lines.append(f"  {label}")
        if decay:
            lines.append(f"⚠️ 趨勢疲軟:")
            for t in decay[:5]:
                label = _get_stock_label_from_df(scored_df, t)
                lines.append(f"  {label}")

    return "\n".join(lines)
