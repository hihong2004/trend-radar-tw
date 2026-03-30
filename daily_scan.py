"""
daily_scan.py — 台股每日趨勢掃描
GitHub Actions 入口，台灣收盤後自動執行。

Usage:
  python daily_scan.py --dry-run
  python daily_scan.py --always-send
  python daily_scan.py --full
"""

import sys
import argparse
import logging
import json
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def load_theme_info():
    import config
    path = os.path.join(config.BASE_DIR, "themes", "theme_groups.json")
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def main():
    parser = argparse.ArgumentParser(description="台股趨勢雷達 — 每日掃描")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--always-send", action="store_true")
    args = parser.parse_args()

    try:
        logger.info("=" * 50)
        logger.info("🚀 開始台股每日趨勢掃描")
        logger.info("=" * 50)

        from data_pipeline import load_all_data
        from scoring.composite import (
            score_all_tickers, determine_stages,
            get_stage_transitions, get_watchlist, save_daily_snapshot,
        )
        from alerts.formatter_tw import format_daily_report, format_short_alert
        from alerts.line_alert import send_line_alert

        data = load_all_data(full_refresh=args.full)

        if data["ohlcv"].empty:
            logger.error("❌ OHLCV 為空")
            sys.exit(1)

        logger.info(f"  {data['ohlcv']['ticker'].nunique()} 檔數據")

        logger.info("📊 九維評分...")
        scored_df = score_all_tickers(data)

        if scored_df.empty:
            logger.error("❌ 評分結果為空")
            sys.exit(1)

        logger.info(f"  {len(scored_df)} 檔完成評分")

        logger.info("🔄 Stage 判定...")
        scored_df = determine_stages(scored_df)

        transitions = get_stage_transitions(scored_df)
        logger.info(f"  新 Stage 1: {len(transitions['new_stage1'])}")
        logger.info(f"  新 Stage 2: {len(transitions['new_stage2'])}")
        logger.info(f"  衰退: {len(transitions['decay_stage3'])}")

        theme_info = load_theme_info()

        has_changes = bool(
            transitions["new_stage1"] or transitions["new_stage2"] or transitions["decay_stage3"]
        )

        if has_changes:
            report = format_daily_report(scored_df, transitions, theme_info)
        else:
            report = format_short_alert(scored_df, transitions)

        save_daily_snapshot(scored_df)

        if args.dry_run:
            logger.info("📋 [DRY RUN]：")
            print("\n" + report + "\n")

            watchlist = get_watchlist(scored_df, min_stars=4)
            logger.info(f"★★★★+ 觀察名單: {len(watchlist)} 檔")
            for _, r in watchlist.head(15).iterrows():
                logger.info(
                    f"  {r.get('stock_id',''):4s} {r.get('name','')[:4]:4s} "
                    f"{r['total_score']:5.1f} {'★'*r['stars']} St{r.get('stage','?')}"
                )
        else:
            should_send = args.always_send or has_changes or (scored_df["stars"] >= 4).sum() > 0
            if should_send:
                logger.info("📤 發送 LINE...")
                success = send_line_alert(report)
                if not success:
                    logger.error("❌ LINE 失敗")
                    sys.exit(1)
            else:
                logger.info("📭 無變化，不推播")

        logger.info("🏁 完成")

    except Exception as e:
        logger.exception(f"💥 失敗: {e}")
        if not args.dry_run:
            try:
                from alerts.line_alert import send_line_alert
                send_line_alert(f"⚠️ 台股趨勢雷達錯誤\n\n{str(e)[:500]}")
            except Exception:
                pass
        sys.exit(1)


if __name__ == "__main__":
    main()
