"""
dashboard.py — 台股趨勢雷達儀表板
5 個分頁：趨勢雷達、產業熱力圖、主題追蹤、個股詳情、系統表現
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
import json
import os
import sys
import logging

logging.basicConfig(level=logging.INFO, stream=sys.stdout)

st.set_page_config(page_title="台股趨勢雷達", page_icon="📡", layout="wide")

st.markdown("""
<style>
    .stApp { background-color: #0E1117; }
    .section-header {
        font-size: 1.2rem; font-weight: bold; color: #E0E0E0;
        border-bottom: 2px solid #2D3250; padding-bottom: 6px; margin: 15px 0 8px 0;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=3600, show_spinner="載入數據中...")
def load_data():
    from data_pipeline import load_all_data
    return load_all_data(full_refresh=False)


@st.cache_data(ttl=3600, show_spinner="九維評分中...")
def compute_scores(_data):
    from scoring.composite import score_all_tickers, determine_stages
    scored = score_all_tickers(_data)
    if not scored.empty:
        scored = determine_stages(scored)
    return scored


def load_themes():
    import config
    path = os.path.join(config.BASE_DIR, "themes", "theme_groups.json")
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"active_themes": [], "cooling_themes": []}


def make_radar_chart(dims, title=""):
    labels = {
        "relative_strength": "相對強度",
        "price_structure": "價格結構",
        "volume_analysis": "成交量",
        "volatility": "波動率",
        "sector_momentum": "族群共振",
        "trend_consistency": "趨勢持續",
        "theme_momentum": "主題熱度",
        "institutional_flow": "法人動向",
        "revenue_momentum": "營收動能",
    }
    cats = list(labels.values())
    vals = [dims.get(k, 0) for k in labels.keys()]
    vals.append(vals[0])
    cats.append(cats[0])

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=vals, theta=cats, fill="toself",
        fillcolor="rgba(0,212,170,0.2)",
        line=dict(color="#00D4AA", width=2),
    ))
    fig.update_layout(
        polar=dict(
            radialaxis=dict(visible=True, range=[0, 100], tickfont=dict(size=8, color="#666")),
            bgcolor="rgba(0,0,0,0)",
            angularaxis=dict(tickfont=dict(size=9, color="#AAA")),
        ),
        showlegend=False, height=320,
        margin=dict(l=60, r=60, t=30, b=30),
        paper_bgcolor="rgba(0,0,0,0)", font=dict(color="white"),
    )
    return fig


def make_price_chart(ohlcv, ticker, days=252):
    from data_pipeline import get_ticker_df
    df = get_ticker_df(ohlcv, ticker)
    if df.empty:
        return go.Figure()
    df = df.tail(days)
    df["ma50"] = df["close"].rolling(50).mean()
    df["ma200"] = df["close"].rolling(200).mean()

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["date"], y=df["close"], name="Close", line=dict(color="#00D4AA", width=1.5)))
    fig.add_trace(go.Scatter(x=df["date"], y=df["ma50"], name="50MA", line=dict(color="#FFD93D", width=1, dash="dash")))
    fig.add_trace(go.Scatter(x=df["date"], y=df["ma200"], name="200MA", line=dict(color="#FF6B6B", width=1, dash="dash")))
    fig.update_layout(
        height=350, margin=dict(l=50, r=20, t=30, b=30),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(14,17,23,1)",
        font=dict(color="#AAA"), xaxis=dict(gridcolor="#1E2130"), yaxis=dict(gridcolor="#1E2130"),
        legend=dict(orientation="h", y=1.1),
    )
    return fig


def main():
    with st.sidebar:
        st.title("📡 台股趨勢雷達")
        if st.button("🔄 重新載入", type="primary", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
        st.markdown("---")
        min_stars = st.slider("最低星級", 1, 5, 3)
        stage_filter = st.multiselect(
            "Stage", [0, 1, 2, 3], default=[1, 2],
            format_func=lambda x: {0: "💤沉睡", 1: "🌅覺醒", 2: "🚀加速", 3: "⚠️衰退"}[x],
        )
        st.markdown("---")
        st.markdown("### 九維評分")
        st.markdown("RS 18% | 價格 15% | 量能 14%\n波動 10% | 族群 10% | 持續 8%\n主題 7% | **法人 10%** | **營收 8%**")

    try:
        data = load_data()
        scored_df = compute_scores(data)
    except Exception as e:
        st.error(f"載入失敗: {e}")
        return

    if scored_df.empty:
        st.warning("評分結果為空")
        return

    themes = load_themes()

    filtered = scored_df[scored_df["stars"] >= min_stars]
    if stage_filter and "stage" in filtered.columns:
        filtered = filtered[filtered["stage"].isin(stage_filter)]

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📡 趨勢雷達", "🏭 產業熱力圖", "🌐 主題追蹤", "📊 個股詳情", "📈 系統表現"
    ])

    # ── Tab 1: 趨勢雷達 ──
    with tab1:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("掃描標的", len(scored_df))
        s1 = (scored_df["stage"] == 1).sum() if "stage" in scored_df.columns else 0
        s2 = (scored_df["stage"] == 2).sum() if "stage" in scored_df.columns else 0
        c2.metric("🌅 Stage 1", s1)
        c3.metric("🚀 Stage 2", s2)
        c4.metric("★★★★+", (scored_df["stars"] >= 4).sum())

        st.markdown("---")

        if filtered.empty:
            st.info("無符合條件的標的")
        else:
            cols = ["stock_id", "name", "total_score", "stars", "sector", "price"]
            if "stage" in filtered.columns:
                cols.insert(4, "stage")

            display = filtered[cols].copy()
            display = display.rename(columns={
                "stock_id": "代號", "name": "名稱", "total_score": "總分",
                "stars": "星級", "sector": "產業", "stage": "Stage", "price": "股價",
            })
            display["星級"] = display["星級"].apply(lambda x: "★" * x + "☆" * (5 - x))
            if "Stage" in display.columns:
                display["Stage"] = display["Stage"].map({0: "💤", 1: "🌅", 2: "🚀", 3: "⚠️"}).fillna("?")

            st.dataframe(display, hide_index=True, use_container_width=True, height=600)

    # ── Tab 2: 產業熱力圖 ──
    with tab2:
        st.markdown('<div class="section-header">產業 × 評分維度</div>', unsafe_allow_html=True)

        if "sector" in scored_df.columns and "dimensions" in scored_df.columns:
            dim_names = list(config_weights().keys()) if hasattr(config_weights, '__call__') else [
                "relative_strength", "price_structure", "volume_analysis",
                "volatility", "sector_momentum", "trend_consistency",
                "theme_momentum", "institutional_flow", "revenue_momentum",
            ]
            dim_labels = ["RS", "價格", "量能", "波動", "族群", "持續", "主題", "法人", "營收"]

            sector_avgs = []
            for sector, group in scored_df.groupby("sector"):
                if len(group) < 3:
                    continue
                row = {"sector": sector}
                for dim in dim_names:
                    vals = group["dimensions"].apply(lambda d: d.get(dim, 0) if isinstance(d, dict) else 0)
                    row[dim] = vals.mean()
                sector_avgs.append(row)

            if sector_avgs:
                hm = pd.DataFrame(sector_avgs).set_index("sector")
                hm.columns = dim_labels

                fig = px.imshow(
                    hm.values, x=dim_labels, y=hm.index.tolist(),
                    color_continuous_scale="RdYlGn", zmin=20, zmax=80,
                    text_auto=".0f", aspect="auto",
                )
                fig.update_layout(
                    height=600, margin=dict(l=150, r=20, t=30, b=50),
                    paper_bgcolor="rgba(0,0,0,0)", font=dict(color="#AAA"),
                )
                st.plotly_chart(fig, use_container_width=True)

    # ── Tab 3: 主題追蹤 ──
    with tab3:
        active = themes.get("active_themes", [])
        st.markdown(f"**最後更新：** {themes.get('last_updated', '尚未掃描')}")

        if not active:
            st.info("主題引擎尚未執行。執行 weekly_themes.py 後出現數據。")
        else:
            st.markdown('<div class="section-header">🔥 升溫主題</div>', unsafe_allow_html=True)
            for t in active:
                acc = t.get("acceleration", 0)
                tickers = t.get("tickers", [])
                with st.expander(f"🔺 {t.get('theme', '?')} — {acc:.1f}x"):
                    st.markdown(f"**分類：** {t.get('category', '')}")
                    st.markdown(f"**關聯企業：** {', '.join(tickers)}")
                    if t.get("reasoning"):
                        st.markdown(f"**分析：** {t['reasoning']}")

                    if tickers and not scored_df.empty:
                        # 從 stock_id 找到這些股票
                        theme_stocks = scored_df[scored_df["stock_id"].isin(tickers)]
                        if not theme_stocks.empty:
                            st.dataframe(
                                theme_stocks[["stock_id", "name", "total_score", "stars", "price"]].rename(
                                    columns={"stock_id": "代號", "name": "名稱", "total_score": "總分", "stars": "星級", "price": "股價"}
                                ), hide_index=True,
                            )

    # ── Tab 4: 個股詳情 ──
    with tab4:
        options = scored_df.apply(lambda r: f"{r.get('stock_id','')} {r.get('name','')}", axis=1).tolist()
        selected_idx = st.selectbox("選擇標的", range(len(options)), format_func=lambda i: options[i])

        if selected_idx is not None:
            row = scored_df.iloc[selected_idx]

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("總分", f"{row['total_score']:.1f}")
            c2.metric("星級", "★" * row["stars"])
            c3.metric("股價", f"${row['price']:,.2f}")
            stage_text = {0: "💤", 1: "🌅", 2: "🚀", 3: "⚠️"}.get(row.get("stage"), "?")
            c4.metric("Stage", stage_text)
            c5.metric("產業", row.get("sector", "")[:8])

            st.markdown("---")

            col1, col2 = st.columns(2)
            dims = row.get("dimensions", {})
            if isinstance(dims, dict):
                with col1:
                    fig = make_radar_chart(dims, row.get("stock_id", ""))
                    st.plotly_chart(fig, use_container_width=True)
                with col2:
                    # 法人與營收摘要
                    st.markdown("#### 📋 評分細節")
                    details = row.get("details", {})
                    if isinstance(details, dict):
                        for k, v in details.items():
                            label = {"rs": "相對強度", "price": "價格結構", "volume": "成交量",
                                     "volatility": "波動率", "sector": "族群共振", "consistency": "趨勢持續",
                                     "theme": "主題熱度", "institutional": "⭐法人動向", "revenue": "⭐營收動能"}.get(k, k)
                            st.markdown(f"**{label}:** {v}")

            st.markdown('<div class="section-header">📈 價格走勢</div>', unsafe_allow_html=True)
            chart_days = st.select_slider(
                "回看", options=[60, 120, 252, 504], value=252,
                format_func=lambda x: {60: "3月", 120: "半年", 252: "1年", 504: "2年"}[x],
            )
            fig = make_price_chart(data["ohlcv"], row["ticker"], chart_days)
            st.plotly_chart(fig, use_container_width=True)

    # ── Tab 5: 系統表現 ──
    with tab5:
        st.markdown('<div class="section-header">📈 系統歷史表現</div>', unsafe_allow_html=True)
        st.info("系統運行數天後開始累積數據。點下方按鈕計算。")

        if st.button("🔄 計算歷史表現"):
            try:
                from performance_tracker import track_performance, compute_hit_rates, format_performance_summary
                with st.spinner("計算中..."):
                    perf = track_performance(data["ohlcv"])
                    if perf.empty:
                        st.info("尚無歷史快照")
                    else:
                        stats = compute_hit_rates(perf)
                        st.text(format_performance_summary(stats))
            except Exception as e:
                st.error(f"失敗: {e}")

    st.markdown("---")
    st.markdown('<p style="text-align:center;color:#555;">📡 Trend Radar TW v1.0 | Yahoo Finance + 證交所 + Google Trends</p>', unsafe_allow_html=True)


def config_weights():
    import config
    return config.SCORING_WEIGHTS


if __name__ == "__main__":
    main()
