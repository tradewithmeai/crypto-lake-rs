"""
Gap Analysis for Crypto Lake Parquet Data

Scans all parquet files, identifies genuine gaps (missing seconds) in
1-second bar coverage, distinguishes from no-trade bars (trade_count=0),
classifies likely causes, and produces a detailed report with source breakdown.
"""

import duckdb
import os
from datetime import datetime, timezone, timedelta

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "parquet")
REPORT_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "reports", "gap_analysis.md")

# A gap larger than this (seconds) is considered significant
GAP_THRESHOLD = 5


def main():
    con = duckdb.connect()

    # Discover all exchange/symbol combos
    exchanges = sorted([d for d in os.listdir(DATA_DIR) if os.path.isdir(os.path.join(DATA_DIR, d))])

    all_gaps = []
    summaries = []

    for exchange in exchanges:
        exchange_dir = os.path.join(DATA_DIR, exchange)
        symbols = sorted([d for d in os.listdir(exchange_dir) if os.path.isdir(os.path.join(exchange_dir, d))])

        for symbol in symbols:
            symbol_dir = os.path.join(exchange_dir, symbol)
            glob_pattern = symbol_dir.replace("\\", "/") + "/**/*.parquet"

            # Check if source column exists
            try:
                cols = con.execute(f"""
                    SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('{glob_pattern}', hive_partitioning=true))
                """).fetchall()
                col_names = [c[0] for c in cols]
                has_source = "source" in col_names
            except Exception:
                has_source = False

            try:
                if has_source:
                    result = con.execute(f"""
                        SELECT
                            epoch_us(window_start) / 1000000 as ts_sec,
                            trade_count,
                            source
                        FROM read_parquet('{glob_pattern}', hive_partitioning=true)
                        ORDER BY ts_sec
                    """).fetchall()
                else:
                    result = con.execute(f"""
                        SELECT
                            epoch_us(window_start) / 1000000 as ts_sec,
                            trade_count,
                            'unknown' as source
                        FROM read_parquet('{glob_pattern}', hive_partitioning=true)
                        ORDER BY ts_sec
                    """).fetchall()
            except Exception as e:
                print(f"  Error reading {exchange}/{symbol}: {e}")
                continue

            if len(result) < 2:
                continue

            timestamps = [r[0] for r in result]
            trade_counts = [r[1] for r in result]
            sources = [r[2] or "unknown" for r in result]
            first_ts = timestamps[0]
            last_ts = timestamps[-1]
            total_bars = len(timestamps)
            span_secs = last_ts - first_ts
            bars_with_trades = sum(1 for tc in trade_counts if tc > 0)
            no_trade_bars = total_bars - bars_with_trades

            # Source breakdown
            source_counts = {}
            for s in sources:
                source_counts[s] = source_counts.get(s, 0) + 1

            # Find genuine gaps (missing seconds, not just trade_count=0)
            gaps = []
            for i in range(1, len(timestamps)):
                diff = timestamps[i] - timestamps[i - 1]
                if diff > GAP_THRESHOLD:
                    gap_start = datetime.fromtimestamp(timestamps[i - 1], tz=timezone.utc)
                    gap_end = datetime.fromtimestamp(timestamps[i], tz=timezone.utc)
                    gaps.append({
                        "exchange": exchange,
                        "symbol": symbol,
                        "start": gap_start,
                        "end": gap_end,
                        "duration_secs": diff,
                        "start_ts": timestamps[i - 1],
                        "end_ts": timestamps[i],
                    })

            # Classify gaps
            for gap in gaps:
                gap["cause"] = classify_gap(gap)

            all_gaps.extend(gaps)

            # Coverage: bars present / total span
            total_gap_secs = sum(g["duration_secs"] for g in gaps)
            coverage = ((span_secs - total_gap_secs) / span_secs * 100) if span_secs > 0 else 0
            trade_pct = (bars_with_trades / total_bars * 100) if total_bars > 0 else 0

            summaries.append({
                "exchange": exchange,
                "symbol": symbol,
                "first": datetime.fromtimestamp(first_ts, tz=timezone.utc),
                "last": datetime.fromtimestamp(last_ts, tz=timezone.utc),
                "total_bars": total_bars,
                "bars_with_trades": bars_with_trades,
                "no_trade_bars": no_trade_bars,
                "span_secs": span_secs,
                "gap_count": len(gaps),
                "total_gap_secs": total_gap_secs,
                "coverage": coverage,
                "trade_pct": trade_pct,
                "source_counts": source_counts,
            })

            print(f"  {exchange}/{symbol}: {len(gaps)} gaps, {coverage:.2f}% coverage, "
                  f"{no_trade_bars} no-trade bars ({trade_pct:.1f}% active)")

    # Cross-reference gaps across symbols to find correlated outages
    correlated = find_correlated_gaps(all_gaps)

    # Generate report
    report = generate_report(summaries, all_gaps, correlated)

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"\nReport written to: {REPORT_PATH}")


def classify_gap(gap):
    """Classify the likely cause of a gap based on its characteristics."""
    dur = gap["duration_secs"]

    if dur > 3600:
        return "Computer shutdown/restart"
    if dur > 300:
        return "App restart or extended network outage"
    if dur > 60:
        return "Network switch or brief disconnect"
    if dur > GAP_THRESHOLD:
        return "WebSocket reconnect"
    return "Unknown"


def find_correlated_gaps(all_gaps):
    """Find gaps that overlap across multiple symbols/exchanges, suggesting a common cause."""
    if not all_gaps:
        return []

    sorted_gaps = sorted(all_gaps, key=lambda g: g["start_ts"])
    clusters = []
    current_cluster = [sorted_gaps[0]]
    cluster_end = sorted_gaps[0]["end_ts"]

    for gap in sorted_gaps[1:]:
        if gap["start_ts"] <= cluster_end + 30:
            current_cluster.append(gap)
            cluster_end = max(cluster_end, gap["end_ts"])
        else:
            if len(current_cluster) > 1:
                clusters.append(current_cluster)
            current_cluster = [gap]
            cluster_end = gap["end_ts"]

    if len(current_cluster) > 1:
        clusters.append(current_cluster)

    correlated = []
    for cluster in clusters:
        exchanges = set(g["exchange"] for g in cluster)
        symbols = set(f"{g['exchange']}/{g['symbol']}" for g in cluster)
        earliest = min(g["start"] for g in cluster)
        latest = max(g["end"] for g in cluster)
        avg_dur = sum(g["duration_secs"] for g in cluster) / len(cluster)

        cause = "Computer shutdown/restart" if avg_dur > 3600 else \
                "App restart" if avg_dur > 300 else \
                "Network switch" if len(exchanges) > 1 else \
                "Exchange-specific outage"

        correlated.append({
            "start": earliest,
            "end": latest,
            "num_affected": len(symbols),
            "exchanges": exchanges,
            "symbols": symbols,
            "avg_duration": avg_dur,
            "likely_cause": cause,
        })

    return correlated


def fmt_duration(secs):
    """Format seconds into human-readable duration."""
    if secs >= 86400:
        d = secs // 86400
        h = (secs % 86400) // 3600
        return f"{d}d {h}h"
    if secs >= 3600:
        h = secs // 3600
        m = (secs % 3600) // 60
        return f"{h}h {m}m"
    if secs >= 60:
        m = secs // 60
        s = secs % 60
        return f"{m}m {s}s"
    return f"{secs}s"


def generate_report(summaries, all_gaps, correlated):
    lines = []
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines.append(f"# Crypto Lake - Data Gap Analysis Report")
    lines.append(f"Generated: {now}\n")

    # Overall Summary
    lines.append("## Overall Summary\n")
    if summaries:
        first_data = min(s["first"] for s in summaries)
        last_data = max(s["last"] for s in summaries)
        total_bars = sum(s["total_bars"] for s in summaries)
        total_with_trades = sum(s["bars_with_trades"] for s in summaries)
        total_no_trade = sum(s["no_trade_bars"] for s in summaries)
        total_gaps = sum(s["gap_count"] for s in summaries)

        # Aggregate source counts
        agg_sources = {}
        for s in summaries:
            for src, cnt in s["source_counts"].items():
                agg_sources[src] = agg_sources.get(src, 0) + cnt

        lines.append(f"- **Collection period**: {first_data.strftime('%Y-%m-%d %H:%M')} to {last_data.strftime('%Y-%m-%d %H:%M')} UTC")
        lines.append(f"- **Total span**: {fmt_duration(int((last_data - first_data).total_seconds()))}")
        lines.append(f"- **Total 1s bars**: {total_bars:,}")
        lines.append(f"- **Bars with trades**: {total_with_trades:,} ({total_with_trades/total_bars*100:.1f}%)")
        lines.append(f"- **No-trade bars**: {total_no_trade:,} ({total_no_trade/total_bars*100:.1f}%)")
        lines.append(f"- **Genuine gaps (missing data)**: {total_gaps}")
        lines.append(f"- **Exchanges**: {len(set(s['exchange'] for s in summaries))}")
        lines.append(f"- **Symbols**: {len(summaries)}")
        lines.append("")

        # Source breakdown
        lines.append("### Data Source Breakdown\n")
        lines.append("| Source | Bars | Percentage |")
        lines.append("|--------|------|-----------|")
        for src in sorted(agg_sources.keys()):
            cnt = agg_sources[src]
            pct = cnt / total_bars * 100
            lines.append(f"| {src} | {cnt:,} | {pct:.1f}% |")
        lines.append("")

    # Coverage by Symbol
    lines.append("## Coverage by Symbol\n")
    lines.append("| Exchange | Symbol | First Bar | Last Bar | Total Bars | With Trades | No-Trade | Gaps | Gap Time | Coverage | Sources |")
    lines.append("|----------|--------|-----------|----------|------------|-------------|----------|------|----------|----------|---------|")
    for s in sorted(summaries, key=lambda x: (x["exchange"], x["symbol"])):
        # Compact source summary
        src_parts = []
        for src in sorted(s["source_counts"].keys()):
            cnt = s["source_counts"][src]
            src_parts.append(f"{src}:{cnt:,}")
        src_str = " ".join(src_parts)

        lines.append(
            f"| {s['exchange']} | {s['symbol']} | {s['first'].strftime('%m-%d %H:%M')} "
            f"| {s['last'].strftime('%m-%d %H:%M')} | {s['total_bars']:,} "
            f"| {s['bars_with_trades']:,} | {s['no_trade_bars']:,} "
            f"| {s['gap_count']} "
            f"| {fmt_duration(int(s['total_gap_secs']))} | {s['coverage']:.2f}% "
            f"| {src_str} |"
        )
    lines.append("")

    # Correlated Outages
    if correlated:
        lines.append("## Correlated Outages (affecting multiple symbols)\n")
        lines.append("These gaps occurred simultaneously across multiple symbols, indicating a shared root cause.\n")

        for i, c in enumerate(sorted(correlated, key=lambda x: x["start"]), 1):
            lines.append(f"### Outage {i}: {c['start'].strftime('%Y-%m-%d %H:%M:%S')} UTC")
            lines.append(f"- **Time**: {c['start'].strftime('%Y-%m-%d %H:%M:%S')} -> {c['end'].strftime('%Y-%m-%d %H:%M:%S')} UTC")
            lines.append(f"- **Avg duration**: {fmt_duration(int(c['avg_duration']))}")
            lines.append(f"- **Symbols affected**: {c['num_affected']}")
            lines.append(f"- **Exchanges**: {', '.join(sorted(c['exchanges']))}")
            lines.append(f"- **Likely cause**: {c['likely_cause']}")
            lines.append("")

    # Gap Cause Breakdown
    lines.append("## Gap Cause Breakdown\n")
    if all_gaps:
        cause_counts = {}
        cause_durations = {}
        for g in all_gaps:
            c = g["cause"]
            cause_counts[c] = cause_counts.get(c, 0) + 1
            cause_durations[c] = cause_durations.get(c, 0) + g["duration_secs"]

        lines.append("| Cause | Count | Total Duration | Avg Duration |")
        lines.append("|-------|-------|---------------|-------------|")
        for cause in sorted(cause_counts.keys(), key=lambda c: -cause_durations[c]):
            cnt = cause_counts[cause]
            total = cause_durations[cause]
            avg = total / cnt
            lines.append(f"| {cause} | {cnt} | {fmt_duration(int(total))} | {fmt_duration(int(avg))} |")
    else:
        lines.append("No gaps detected. 100% coverage.")
    lines.append("")

    # Detailed Gap List
    lines.append("## All Gaps (sorted by duration)\n")
    if all_gaps:
        lines.append("| Exchange | Symbol | Start (UTC) | End (UTC) | Duration | Cause |")
        lines.append("|----------|--------|-------------|-----------|----------|-------|")

        sorted_gaps = sorted(all_gaps, key=lambda g: -g["duration_secs"])
        for g in sorted_gaps[:200]:
            lines.append(
                f"| {g['exchange']} | {g['symbol']} "
                f"| {g['start'].strftime('%Y-%m-%d %H:%M:%S')} "
                f"| {g['end'].strftime('%Y-%m-%d %H:%M:%S')} "
                f"| {fmt_duration(int(g['duration_secs']))} "
                f"| {g['cause']} |"
            )

        if len(sorted_gaps) > 200:
            lines.append(f"\n*...and {len(sorted_gaps) - 200} more shorter gaps*\n")
    else:
        lines.append("No gaps detected.")
    lines.append("")

    # Recommendations
    lines.append("## Recommendations\n")

    has_shutdown = any(g["cause"] == "Computer shutdown/restart" for g in all_gaps)
    has_app_restart = any(g["cause"] == "App restart or extended network outage" for g in all_gaps)
    has_network = any(g["cause"] == "Network switch or brief disconnect" for g in all_gaps)

    rec_num = 1
    if has_shutdown:
        lines.append(f"{rec_num}. **Enable auto-start on boot** -- Use `--install-autostart` or toggle in the tray menu to prevent gaps from computer restarts.")
        rec_num += 1
        lines.append(f"{rec_num}. **Startup backfill is active** -- The backfill feature fills gaps from REST APIs on startup. Verify it's working by checking for `_backfill` parquet files after restart.")
        rec_num += 1

    if has_app_restart:
        lines.append(f"{rec_num}. **Investigate app crashes** -- Check `crash.log` next to the executable for panic traces. Also check Windows Event Viewer for crash reports.")
        rec_num += 1

    if has_network:
        lines.append(f"{rec_num}. **Network resilience** -- Current reconnect backoff is 2s. Consider a wired connection or UPS for the router to minimize network-related gaps.")
        rec_num += 1

    if not all_gaps:
        lines.append(f"{rec_num}. **Perfect coverage** -- No gaps detected. The collector is running continuously with no data loss.")
        rec_num += 1

    lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    main()
