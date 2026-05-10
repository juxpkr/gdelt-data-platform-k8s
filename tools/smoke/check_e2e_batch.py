#!/usr/bin/env python3
"""k3s E2E pipeline smoke test.

실제 배포된 Trino에 쿼리해서 최신 E2E complete batch(bronze/silver/gold 모두 success)가
정상인지 검증한다. CI 또는 수동 운영 점검용.
"""
import argparse
import csv
import io
import subprocess
import sys
from dataclasses import dataclass

# ─── ANSI ────────────────────────────────────────────────────────────────────

OK   = "\033[0;32m[OK]\033[0m"
FAIL = "\033[0;31m[FAIL]\033[0m"
INFO = "\033[0;34m[INFO]\033[0m"


# ─── Trino query ─────────────────────────────────────────────────────────────

def _trino_exec(sql: str, namespace: str, trino_deploy: str) -> str:
    cmd = [
        "kubectl", "exec", "-n", namespace, trino_deploy,
        "--", "trino", "--execute", sql,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"trino command failed (exit {result.returncode}):\n{result.stderr.strip()}"
        )
    return result.stdout.strip()


def _parse_rows(output: str) -> list[list[str]]:
    """Trino CLI CSV 출력 → list of row lists (따옴표 제거)."""
    if not output:
        return []
    reader = csv.reader(io.StringIO(output))
    return list(reader)


@dataclass
class StageRow:
    stage: str
    status: str
    input_rows: int
    output_rows: int
    duration_seconds: float
    started_at: str
    finished_at: str


# ─── Checks ──────────────────────────────────────────────────────────────────

def find_latest_e2e_batch(namespace: str, trino_deploy: str) -> str | None:
    sql = """
    SELECT batch_id
    FROM nessie.audit.pipeline_batch_runs
    GROUP BY batch_id
    HAVING COUNT(DISTINCT stage) = 3
       AND SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) = 3
    ORDER BY MAX(finished_at) DESC
    LIMIT 1
    """
    rows = _parse_rows(_trino_exec(sql, namespace, trino_deploy))
    return rows[0][0] if rows else None


def fetch_stage_rows(batch_id: str, namespace: str, trino_deploy: str) -> list[StageRow]:
    # stage별 최신 row만 (재실행 시 동일 stage에 여러 row가 생길 수 있음)
    sql = f"""
    SELECT stage, status, input_rows, output_rows, duration_seconds,
           CAST(started_at AS varchar) AS started_at,
           CAST(finished_at AS varchar) AS finished_at
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY stage ORDER BY finished_at DESC) AS rn
        FROM nessie.audit.pipeline_batch_runs
        WHERE batch_id = '{batch_id}'
    ) t
    WHERE rn = 1
    ORDER BY
      CASE stage
        WHEN 'bronze' THEN 1
        WHEN 'silver' THEN 2
        WHEN 'gold'   THEN 3
        ELSE 99
      END
    """
    raw = _parse_rows(_trino_exec(sql, namespace, trino_deploy))
    return [
        StageRow(
            stage=r[0],
            status=r[1],
            input_rows=int(r[2]),
            output_rows=int(r[3]),
            duration_seconds=float(r[4]),
            started_at=r[5],
            finished_at=r[6],
        )
        for r in raw
    ]


def fetch_gold_batch_quality(batch_id: str, namespace: str, trino_deploy: str) -> dict:
    """gold 테이블의 batch 기준 품질 지표를 반환한다."""
    sql = f"""
    SELECT
      COUNT(*)                              AS table_count,
      COUNT(DISTINCT global_event_id)       AS distinct_ids
    FROM nessie.gold.gold_llm_context
    WHERE source_batch_id = '{batch_id}'
    """
    rows = _parse_rows(_trino_exec(sql, namespace, trino_deploy))
    if not rows:
        return {}
    r = rows[0]
    return {
        "table_count": int(r[0]),
        "distinct_ids": int(r[1]),
    }


def fetch_silver_quality(batch_id: str, namespace: str, trino_deploy: str) -> dict:
    """silver 테이블의 데이터 품질 지표를 반환한다."""
    sql = f"""
    SELECT
      COUNT(*)                                                              AS total,
      COUNT(DISTINCT global_event_id)                                       AS distinct_ids,
      SUM(CASE WHEN mention_source_name IS NOT NULL THEN 1 ELSE 0 END)     AS mention_joined,
      SUM(CASE WHEN event_code IS NULL THEN 1 ELSE 0 END)                  AS event_code_nulls
    FROM nessie.silver.gdelt_events_detailed
    WHERE source_batch_id = '{batch_id}'
    """
    rows = _parse_rows(_trino_exec(sql, namespace, trino_deploy))
    if not rows:
        return {}
    r = rows[0]
    return {
        "total":            int(r[0]),
        "distinct_ids":     int(r[1]),
        "mention_joined":   int(r[2]),
        "event_code_nulls": int(r[3]),
    }


def fetch_age_seconds(batch_id: str, namespace: str, trino_deploy: str) -> float:
    sql = f"""
    SELECT to_unixtime(CURRENT_TIMESTAMP) - to_unixtime(MAX(finished_at)) AS age_seconds
    FROM nessie.audit.pipeline_batch_runs
    WHERE batch_id = '{batch_id}'
    """
    rows = _parse_rows(_trino_exec(sql, namespace, trino_deploy))
    if not rows or rows[0][0] in ("", "NULL", "null"):
        return float("inf")
    return float(rows[0][0])


# ─── Output helpers ───────────────────────────────────────────────────────────

def print_stage_table(rows: list[StageRow]) -> None:
    header = f"{'stage':<8} {'status':<9} {'input_rows':>11} {'output_rows':>12} {'duration_s':>11}"
    print(header)
    print("-" * len(header))
    for r in rows:
        print(
            f"{r.stage:<8} {r.status:<9} {r.input_rows:>11,} {r.output_rows:>12,} "
            f"{r.duration_seconds:>10.2f}s"
        )


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="GDELT k3s E2E pipeline smoke test")
    parser.add_argument("--max-age-minutes", type=int, default=30,
                        help="최신 E2E batch의 허용 최대 경과 시간 (분, 기본 30)")
    parser.add_argument("--namespace", default="gdelt",
                        help="k8s namespace (기본 gdelt)")
    parser.add_argument("--trino-deploy", default="deploy/trino",
                        help="Trino deploy 대상 (기본 deploy/trino)")
    args = parser.parse_args()

    max_age_seconds = args.max_age_minutes * 60
    ns = args.namespace
    td = args.trino_deploy
    failed = False

    # ── 1. latest E2E complete batch 찾기 ──
    print(f"{INFO} Querying latest E2E complete batch (namespace={ns}, trino={td}) ...")
    try:
        batch_id = find_latest_e2e_batch(ns, td)
    except RuntimeError as e:
        print(f"{FAIL} Trino query failed: {e}", file=sys.stderr)
        return 1

    if not batch_id:
        print(
            f"{FAIL} No complete E2E batch found. "
            "Expected bronze/silver/gold success rows with same batch_id.",
            file=sys.stderr,
        )
        return 1

    print(f"{OK} Latest complete E2E batch: {batch_id}")
    print()

    # ── 2. stage 상세 출력 ──
    try:
        stages = fetch_stage_rows(batch_id, ns, td)
    except RuntimeError as e:
        print(f"{FAIL} Failed to fetch stage rows: {e}", file=sys.stderr)
        return 1

    print_stage_table(stages)
    print()

    # ── 3. freshness 검증 ──
    try:
        age = fetch_age_seconds(batch_id, ns, td)
    except RuntimeError as e:
        print(f"{FAIL} Failed to fetch freshness: {e}", file=sys.stderr)
        return 1

    if age <= max_age_seconds:
        print(f"{OK} Freshness: {age:.0f}s <= {max_age_seconds}s")
    else:
        print(f"{FAIL} Latest complete batch is stale: {age:.0f}s > {max_age_seconds}s")
        failed = True

    # ── 4. row sanity ──
    stage_map = {r.stage: r for r in stages}

    bronze = stage_map.get("bronze")
    if bronze is None:
        print(f"{FAIL} bronze stage row missing from batch {batch_id}")
        failed = True
    elif bronze.output_rows <= 0:
        print(f"{FAIL} bronze output_rows = {bronze.output_rows} (expected > 0)")
        failed = True
    else:
        print(f"{OK} bronze output_rows: {bronze.output_rows:,}")

    for stage_name in ("silver", "gold"):
        row = stage_map.get(stage_name)
        if row is None:
            print(f"{FAIL} {stage_name} stage row missing from batch {batch_id}")
            failed = True
        elif row.output_rows < 0:
            print(f"{FAIL} {stage_name} output_rows = {row.output_rows} (expected >= 0)")
            failed = True
        else:
            print(f"{OK} {stage_name} output_rows: {row.output_rows:,}")

    for row in stages:
        if row.duration_seconds < 0:
            print(f"{FAIL} {row.stage} duration_seconds = {row.duration_seconds} (expected >= 0)")
            failed = True

    # ── 5. gold batch quality ──
    print()
    print(f"{INFO} Gold batch quality (source_batch_id={batch_id}) ...")
    gold_stage = stage_map.get("gold")
    try:
        gq = fetch_gold_batch_quality(batch_id, ns, td)
    except RuntimeError as e:
        print(f"{FAIL} Failed to query gold batch quality: {e}", file=sys.stderr)
        failed = True
        gq = {}

    if gq:
        table_count = gq["table_count"]
        distinct_ids = gq["distinct_ids"]

        # audit output_rows vs 실제 batch count 일치
        if gold_stage is not None:
            audit_out = gold_stage.output_rows
            if audit_out == table_count:
                print(f"{OK} gold audit output_rows == table batch count: {audit_out:,}")
            else:
                print(
                    f"{FAIL} gold audit output_rows mismatch: "
                    f"audit={audit_out:,}  table_batch_count={table_count:,}"
                )
                failed = True
        else:
            print(f"{OK} gold batch count: {table_count:,} rows")

        # dedup
        if table_count == distinct_ids:
            print(f"{OK} gold global_event_id unique: {table_count:,} rows, no duplicates")
        else:
            dupes = table_count - distinct_ids
            print(
                f"{FAIL} gold global_event_id duplicates: {dupes:,} "
                f"(total={table_count:,}, distinct={distinct_ids:,})"
            )
            failed = True

    # ── 6. silver data quality ──
    print()
    print(f"{INFO} Silver data quality (source_batch_id={batch_id}) ...")
    try:
        sq = fetch_silver_quality(batch_id, ns, td)
    except RuntimeError as e:
        print(f"{FAIL} Failed to query silver quality: {e}", file=sys.stderr)
        failed = True
        sq = {}

    if sq:
        total = sq["total"]
        distinct = sq["distinct_ids"]
        mention_joined = sq["mention_joined"]
        event_code_nulls = sq["event_code_nulls"]

        # dedup 보장
        if total == distinct:
            print(f"{OK} global_event_id unique: {total:,} rows, no duplicates")
        else:
            dupes = total - distinct
            print(f"{FAIL} global_event_id duplicates found: {dupes:,} ({total:,} total, {distinct:,} distinct)")
            failed = True

        # event_code not null
        if event_code_nulls == 0:
            print(f"{OK} event_code: no NULLs")
        else:
            print(f"{FAIL} event_code NULL: {event_code_nulls:,} rows")
            failed = True

        # mention join 완전성 (경고만 — 배치마다 mentions 수가 다를 수 있음)
        mention_rate = mention_joined / total * 100 if total > 0 else 0
        if mention_rate >= 50:
            print(f"{OK} mention join rate: {mention_rate:.1f}% ({mention_joined:,}/{total:,})")
        else:
            WARN = "\033[0;33m[WARN]\033[0m"
            print(f"{WARN} mention join rate low: {mention_rate:.1f}% ({mention_joined:,}/{total:,})")

    print()
    if failed:
        print(f"{FAIL} Smoke test FAILED.")
        return 1

    print(f"{OK} All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
