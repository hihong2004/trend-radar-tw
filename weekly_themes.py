"""
weekly_themes.py — 台股每週主題掃描 + Yahoo 分類更新
"""

import sys
import argparse
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="台股每週主題掃描")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--notify", action="store_true")
    parser.add_argument("--update-yahoo", action="store_true", help="更新 Yahoo 分類")
    args = parser.parse_args()

    try:
        logger.info("=" * 50)
        logger.info("🚀 台股每週主題掃描")
        logger.info("=" * 50)

        # Step 1: 更新 Yahoo 分類（每月一次或手動觸發）
        if args.update_yahoo:
            logger.info("🌐 更新 Yahoo 股市分類...")
            from yahoo_classifier import update_concept_groups_from_yahoo
            groups = update_concept_groups_from_yahoo()
            logger.info(f"  Yahoo 分類更新完成: {len(groups)} 個群組")

        # Step 2: Google Trends 掃描
        from themes.theme_mapper import run_theme_discovery
        scan_result = run_theme_discovery()

        # Step 3: 格式化摘要
        rising = scan_result.get("rising_themes", [])
        mappings = scan_result.get("mappings", [])

        lines = [
            "═" * 20,
            "🌐 台股主題雷達週報",
            f"📅 {scan_result.get('scan_date', 'N/A')}",
            "═" * 20,
        ]

        if not rising:
            lines.append("\n📭 本週無升溫主題")
        else:
            lines.append(f"\n🔥 {len(rising)} 個升溫主題\n")
            for m in mappings:
                theme = m.get("theme", m.get("keyword", ""))
                acc = m.get("acceleration", 0)
                tickers = m.get("tickers", [])
                lines.append(f"🔺 {theme} ({acc:.1f}x)")
                if tickers:
                    lines.append(f"   → {', '.join(tickers[:8])}")
                lines.append("")

        discoveries = scan_result.get("new_discoveries", [])
        if discoveries:
            lines.append("💡 新發現")
            for d in discoveries[:8]:
                lines.append(f"  • {d}")

        lines.append(f"\n{'═' * 20}")
        lines.append("📡 Trend Radar TW")

        summary = "\n".join(lines)

        if args.dry_run:
            print("\n" + summary + "\n")
        else:
            if rising or args.notify:
                try:
                    from alerts.line_alert import send_line_alert
                    send_line_alert(summary)
                    logger.info("✅ LINE 已發送")
                except ImportError:
                    print("\n" + summary + "\n")

        logger.info("🏁 完成")

    except Exception as e:
        logger.exception(f"💥 失敗: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
