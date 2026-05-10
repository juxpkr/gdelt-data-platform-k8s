#!/usr/bin/env python3
"""Bronze → Silver 데이터 품질 smoke test.

실제 k3s 클러스터의 Trino를 통해 nessie.silver.gdelt_events_detailed 테이블을 조회하고
데이터 품질을 검증한다.
"""
import argparse
import csv
import io
import re
import subprocess
import sys

OK   = "\033[0;32m[OK]\033[0m"
FAIL = "\033[0;31m[FAIL]\033[0m"
WARN = "\033[0;33m[WARN]\033[0m"
INFO = "\033[0;34m[INFO]\033[0m"

BATCH_ID_RE = re.compile(r"^\d{14}$")


# ─── Trino helpers ────────────────────────────────────────────────────────────

def _trino_exec(sql: str, ns: str, td: str) -> str:
    cmd = ["kubectl", "exec", "-n", ns, td, "--", "trino", "--execute", sql]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"trino command failed (exit {result.returncode}):\n{result.stderr.strip()}"
        )
    return result.stdout.strip()


def _rows(output: str) -> list[list[str]]:
    if not output:
        return []
    return list(csv.reader(io.StringIO(output)))


def _scalar(output: str, default=None):
    r = _rows(output)
    return r[0][0] if r else default


# ─── batch_id 선정 ────────────────────────────────────────────────────────────

def resolve_batch_id(ns: str, td: str) -> tuple[str | None, str]:
    """E2E complete batch 우선, 없으면 silver 최신 batch fallback.

    Returns (batch_id, source) where source is 'e2e' or 'silver_fallback'.
    """
    e2e_sql = """
    SELECT batch_id
    FROM nessie.audit.pipeline_batch_runs
    GROUP BY batch_id
    HAVING COUNT(DISTINCT stage) = 3
       AND SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) = 3
    ORDER BY MAX(finished_at) DESC
    LIMIT 1
    """
    batch_id = _scalar(_trino_exec(e2e_sql, ns, td))
    if batch_id:
        return batch_id, "e2e"

    fallback_sql = """
    SELECT source_batch_id
    FROM nessie.silver.gdelt_events_detailed
    WHERE source_batch_id IS NOT NULL
    GROUP BY source_batch_id
    ORDER BY MAX(processed_at) DESC
    LIMIT 1
    """
    batch_id = _scalar(_trino_exec(fallback_sql, ns, td))
    return batch_id, "silver_fallback"


# ─── 개별 검증 함수 ───────────────────────────────────────────────────────────

def check_dedup(batch_id: str, ns: str, td: str) -> tuple[bool, str]:
    sql = f"""
    SELECT COUNT(*) AS total, COUNT(DISTINCT global_event_id) AS distinct_ids
    FROM nessie.silver.gdelt_events_detailed
    WHERE source_batch_id = '{batch_id}'
    """
    r = _rows(_trino_exec(sql, ns, td))
    if not r:
        return False, "no rows returned"
    total, distinct = int(r[0][0]), int(r[0][1])
    if total == distinct:
        return True, f"{total:,} rows, no duplicates"

    dupes = total - distinct
    sample_sql = f"""
    SELECT global_event_id, COUNT(*) AS cnt
    FROM nessie.silver.gdelt_events_detailed
    WHERE source_batch_id = '{batch_id}'
    GROUP BY global_event_id
    HAVING COUNT(*) > 1
    ORDER BY cnt DESC
    LIMIT 10
    """
    sample = _rows(_trino_exec(sample_sql, ns, td))
    sample_str = ", ".join(f"{r[0]}(×{r[1]})" for r in sample)
    return False, f"{dupes:,} duplicates (total={total:,}, distinct={distinct:,}) — samples: {sample_str}"


def check_core_nulls(batch_id: str, ns: str, td: str) -> tuple[bool, str]:
    cols = ["global_event_id", "event_date", "event_code", "source_batch_id"]
    cases = ",\n  ".join(
        f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS {c}_nulls"
        for c in cols
    )
    sql = f"""
    SELECT COUNT(*), {cases}
    FROM nessie.silver.gdelt_events_detailed
    WHERE source_batch_id = '{batch_id}'
    """
    r = _rows(_trino_exec(sql, ns, td))
    if not r:
        return False, "no rows returned"
    total = int(r[0][0])
    null_counts = {col: int(r[0][i + 1]) for i, col in enumerate(cols)}
    failures = [f"{col}={cnt} NULLs" for col, cnt in null_counts.items() if cnt > 0]
    if failures:
        return False, f"total={total:,} — " + ", ".join(failures)
    return True, f"total={total:,}, all core columns non-null"


def check_mention_join(batch_id: str, ns: str, td: str) -> tuple[str, str]:
    """'pass' / 'warn' / 'fail' 반환."""
    sql = f"""
    SELECT
      COUNT(*) AS total,
      SUM(CASE WHEN mention_source_name IS NULL THEN 1 ELSE 0 END) AS null_cnt
    FROM nessie.silver.gdelt_events_detailed
    WHERE source_batch_id = '{batch_id}'
    """
    r = _rows(_trino_exec(sql, ns, td))
    if not r:
        return "fail", "no rows"
    total, null_cnt = int(r[0][0]), int(r[0][1])
    if total == 0:
        return "warn", "0 rows in silver"
    ratio = null_cnt / total
    msg = f"mention_source_name NULL ratio: {ratio:.2%} ({null_cnt:,}/{total:,})"
    if ratio >= 0.95:
        return "fail", msg
    if ratio >= 0.70:
        return "warn", msg
    return "pass", msg


def check_gkg_enrichment(batch_id: str, ns: str, td: str) -> tuple[str, str]:
    """GKG 컬럼은 자연스럽게 비어 있을 수 있으므로 warn only."""
    cols = ["v2_persons", "v2_organizations", "v2_enhanced_themes"]
    cases = ",\n  ".join(
        f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS {c}_nulls"
        for c in cols
    )
    sql = f"""
    SELECT COUNT(*), {cases}
    FROM nessie.silver.gdelt_events_detailed
    WHERE source_batch_id = '{batch_id}'
    """
    r = _rows(_trino_exec(sql, ns, td))
    if not r:
        return "warn", "no rows"
    total = int(r[0][0])
    if total == 0:
        return "warn", "0 rows"
    ratios = {col: int(r[0][i + 1]) / total for i, col in enumerate(cols)}
    parts = [f"{col}={v:.0%}" for col, v in ratios.items()]
    if all(v >= 0.95 for v in ratios.values()):
        return "warn", "all GKG columns mostly NULL — " + ", ".join(parts)
    return "pass", "GKG enrichment present — " + ", ".join(parts)


def check_retention_ratio(batch_id: str, ns: str, td: str) -> tuple[str, str]:
    bronze_sql = f"""
    SELECT COUNT(*) FROM nessie.bronze.gdelt_events
    WHERE source_batch_id = '{batch_id}'
    """
    silver_sql = f"""
    SELECT COUNT(*) FROM nessie.silver.gdelt_events_detailed
    WHERE source_batch_id = '{batch_id}'
    """
    bronze_rows = int(_scalar(_trino_exec(bronze_sql, ns, td), "0"))
    silver_rows = int(_scalar(_trino_exec(silver_sql, ns, td), "0"))

    if bronze_rows == 0:
        return "warn", f"bronze_events=0 (batch may not have events data)"

    ratio = silver_rows / bronze_rows
    msg = f"bronze_events={bronze_rows:,}  silver={silver_rows:,}  ratio={ratio:.4f}"

    if ratio < 0.03:
        return "fail", f"{msg}  (< 0.03 threshold — excessive drop)"
    if ratio > 1.0:
        return "fail", f"{msg}  (> 1.0 — row explosion suspected)"
    return "pass", msg


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="Bronze → Silver data quality smoke test")
    parser.add_argument("--namespace",    default="gdelt",       help="k8s namespace")
    parser.add_argument("--trino-deploy", default="deploy/trino", help="Trino deploy target")
    parser.add_argument("--batch-id",     default=None,          help="batch_id 직접 지정 (생략 시 자동 선정)")
    args = parser.parse_args()
    ns, td = args.namespace, args.trino_deploy
    failed = False

    print("=" * 52)
    print("  SILVER QUALITY CHECK")
    print("=" * 52)

    # ── batch_id 선정 ──
    if args.batch_id:
        batch_id, source = args.batch_id, "manual"
    else:
        try:
            batch_id, source = resolve_batch_id(ns, td)
        except RuntimeError as e:
            print(f"{FAIL} Failed to resolve batch_id: {e}", file=sys.stderr)
            return 1

    if not batch_id:
        print(f"{FAIL} No batch_id found (silver table may be empty)", file=sys.stderr)
        return 1

    if not BATCH_ID_RE.match(batch_id):
        print(f"{FAIL} Invalid batch_id format: '{batch_id}' (expected 14 digits)", file=sys.stderr)
        return 1

    print(f"batch_id  : {batch_id}  [{source}]")
    print()

    # ── 1. retention ratio (bronze → silver) ──
    try:
        level, msg = check_retention_ratio(batch_id, ns, td)
    except RuntimeError as e:
        print(f"{FAIL} retention_ratio: query failed — {e}")
        failed = True
    else:
        sym = {"pass": OK, "warn": WARN, "fail": FAIL}[level]
        print(f"{sym} retention_ratio  {msg}")
        if level == "fail":
            failed = True

    # ── 2. dedup ──
    try:
        ok, msg = check_dedup(batch_id, ns, td)
    except RuntimeError as e:
        print(f"{FAIL} dedup: query failed — {e}")
        failed = True
    else:
        print(f"{OK if ok else FAIL} dedup            {msg}")
        if not ok:
            failed = True

    # ── 3. core nulls ──
    try:
        ok, msg = check_core_nulls(batch_id, ns, td)
    except RuntimeError as e:
        print(f"{FAIL} core_nulls: query failed — {e}")
        failed = True
    else:
        print(f"{OK if ok else FAIL} core_nulls       {msg}")
        if not ok:
            failed = True

    # ── 4. mention join completeness ──
    try:
        level, msg = check_mention_join(batch_id, ns, td)
    except RuntimeError as e:
        print(f"{FAIL} mention_join: query failed — {e}")
        failed = True
    else:
        sym = {"pass": OK, "warn": WARN, "fail": FAIL}[level]
        print(f"{sym} mention_join     {msg}")
        if level == "fail":
            failed = True

    # ── 5. GKG enrichment ──
    try:
        level, msg = check_gkg_enrichment(batch_id, ns, td)
    except RuntimeError as e:
        print(f"{WARN} gkg_enrichment: query failed — {e}")
    else:
        sym = {"pass": OK, "warn": WARN, "fail": FAIL}[level]
        print(f"{sym} gkg_enrichment   {msg}")

    # ── 결과 ──
    print()
    print("=" * 52)
    if failed:
        print(f"{FAIL} result: FAIL")
        return 1
    print(f"{OK} result: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
