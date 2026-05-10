#!/usr/bin/env python3
"""Gold 데이터 품질 smoke test.

실제 k3s 클러스터의 Trino를 통해 nessie.gold.gold_llm_context 테이블을 조회하고
데이터 품질을 검증한다.
"""
import argparse
import csv
import io
import os
import subprocess
import sys

OK   = "\033[0;32m[OK]\033[0m"
FAIL = "\033[0;31m[FAIL]\033[0m"
WARN = "\033[0;33m[WARN]\033[0m"
INFO = "\033[0;34m[INFO]\033[0m"

SHORT_TEXT_MAX_RATIO = float(os.getenv("SHORT_TEXT_MAX_RATIO", "0.05"))
SHORT_TEXT_THRESHOLD = 50


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


# ─── 개별 검증 함수 ───────────────────────────────────────────────────────────

def check_row_count(ns: str, td: str) -> tuple[bool, str]:
    sql = "SELECT COUNT(*) FROM nessie.gold.gold_llm_context"
    count = int(_scalar(_trino_exec(sql, ns, td), "0"))
    if count > 0:
        return True, f"{count:,} rows"
    return False, "0 rows — gold table is empty"


def check_dedup(ns: str, td: str) -> tuple[bool, str]:
    sql = """
    SELECT COUNT(*) AS total, COUNT(DISTINCT global_event_id) AS distinct_ids
    FROM nessie.gold.gold_llm_context
    """
    r = _rows(_trino_exec(sql, ns, td))
    if not r:
        return False, "no rows returned"
    total, distinct = int(r[0][0]), int(r[0][1])
    if total == distinct:
        return True, f"{total:,} rows, no duplicates"

    dupes = total - distinct
    sample_sql = """
    SELECT global_event_id, COUNT(*) AS cnt
    FROM nessie.gold.gold_llm_context
    GROUP BY global_event_id
    HAVING COUNT(*) > 1
    ORDER BY cnt DESC
    LIMIT 10
    """
    sample = _rows(_trino_exec(sample_sql, ns, td))
    sample_str = ", ".join(f"{row[0]}(×{row[1]})" for row in sample)
    return False, f"{dupes:,} duplicates (total={total:,}, distinct={distinct:,}) — samples: {sample_str}"


def check_core_nulls(ns: str, td: str) -> tuple[bool, str]:
    cols = ["global_event_id", "event_date", "event_code", "llm_content_text"]
    cases = ",\n  ".join(
        f"SUM(CASE WHEN {c} IS NULL THEN 1 ELSE 0 END) AS {c}_nulls"
        for c in cols
    )
    sql = f"SELECT COUNT(*),\n  {cases}\nFROM nessie.gold.gold_llm_context"
    r = _rows(_trino_exec(sql, ns, td))
    if not r:
        return False, "no rows returned"
    total = int(r[0][0])
    null_counts = {col: int(r[0][i + 1]) for i, col in enumerate(cols)}
    failures = [f"{col}={cnt} NULLs" for col, cnt in null_counts.items() if cnt > 0]
    if failures:
        return False, f"total={total:,} — " + ", ".join(failures)
    return True, f"total={total:,}, all core columns non-null"


def check_llm_text_length(ns: str, td: str) -> tuple[str, str]:
    """짧은 llm_content_text 비율 확인 및 평균 길이 출력."""
    sql = f"""
    SELECT
      COUNT(*)                                                                  AS total,
      AVG(CAST(length(llm_content_text) AS double))                            AS avg_len,
      SUM(CASE WHEN length(llm_content_text) < {SHORT_TEXT_THRESHOLD} THEN 1 ELSE 0 END) AS short_cnt
    FROM nessie.gold.gold_llm_context
    WHERE llm_content_text IS NOT NULL
    """
    r = _rows(_trino_exec(sql, ns, td))
    if not r:
        return "fail", "no rows"
    total = int(r[0][0])
    if total == 0:
        return "warn", "0 non-null llm_content_text rows"

    avg_len = float(r[0][1])
    short_cnt = int(r[0][2])
    ratio = short_cnt / total
    msg = (
        f"avg_length={avg_len:.0f}chars  "
        f"short(<{SHORT_TEXT_THRESHOLD}chars)={short_cnt:,}/{total:,} ({ratio:.2%})"
    )
    if ratio > SHORT_TEXT_MAX_RATIO:
        return "fail", f"{msg}  (threshold={SHORT_TEXT_MAX_RATIO:.0%})"
    return "pass", msg


def check_latest_audit(ns: str, td: str) -> tuple[bool, str]:
    """audit 테이블에서 gold stage 최신 row가 success인지 확인."""
    sql = """
    SELECT status, CAST(finished_at AS varchar), batch_id
    FROM nessie.audit.pipeline_batch_runs
    WHERE stage = 'gold'
    ORDER BY finished_at DESC
    LIMIT 1
    """
    r = _rows(_trino_exec(sql, ns, td))
    if not r:
        return False, "no gold audit rows found"
    status, finished_at, batch_id = r[0][0], r[0][1], r[0][2]
    msg = f"status={status}  finished_at={finished_at}  batch_id={batch_id}"
    return status == "success", msg


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="Gold data quality smoke test")
    parser.add_argument("--namespace",    default="gdelt",       help="k8s namespace")
    parser.add_argument("--trino-deploy", default="deploy/trino", help="Trino deploy target")
    args = parser.parse_args()
    ns, td = args.namespace, args.trino_deploy
    failed = False

    print("=" * 52)
    print("  GOLD QUALITY CHECK")
    print("=" * 52)
    print(f"SHORT_TEXT_MAX_RATIO threshold: {SHORT_TEXT_MAX_RATIO:.0%}")
    print()

    # ── 1. row count ──
    try:
        ok, msg = check_row_count(ns, td)
    except RuntimeError as e:
        print(f"{FAIL} row_count: query failed — {e}")
        return 1
    print(f"{OK if ok else FAIL} row_count        {msg}")
    if not ok:
        return 1

    # ── 2. dedup ──
    try:
        ok, msg = check_dedup(ns, td)
    except RuntimeError as e:
        print(f"{FAIL} dedup: query failed — {e}")
        failed = True
    else:
        print(f"{OK if ok else FAIL} dedup            {msg}")
        if not ok:
            failed = True

    # ── 3. core nulls ──
    try:
        ok, msg = check_core_nulls(ns, td)
    except RuntimeError as e:
        print(f"{FAIL} core_nulls: query failed — {e}")
        failed = True
    else:
        print(f"{OK if ok else FAIL} core_nulls       {msg}")
        if not ok:
            failed = True

    # ── 4. llm_content_text 길이 ──
    try:
        level, msg = check_llm_text_length(ns, td)
    except RuntimeError as e:
        print(f"{FAIL} llm_text_length: query failed — {e}")
        failed = True
    else:
        sym = {"pass": OK, "warn": WARN, "fail": FAIL}[level]
        print(f"{sym} llm_text_length  {msg}")
        if level == "fail":
            failed = True

    # ── 5. latest audit ──
    try:
        ok, msg = check_latest_audit(ns, td)
    except RuntimeError as e:
        print(f"{FAIL} latest_audit: query failed — {e}")
        failed = True
    else:
        print(f"{OK if ok else FAIL} latest_audit     {msg}")
        if not ok:
            failed = True

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
