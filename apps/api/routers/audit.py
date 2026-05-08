import re
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from db.trino import fetch_all
from models.schemas import AuditRunsResponse, AuditRunItem, AuditBatchDetail

router = APIRouter()

_ALLOWED_STAGES = {"bronze", "silver", "gold"}
_ALLOWED_STATUSES = {"success", "failed"}
_BATCH_ID_RE = re.compile(r"^\d{14}$")


def _to_run(r: dict) -> AuditRunItem:
    return AuditRunItem(
        batch_id=r["batch_id"],
        stage=r["stage"],
        status=r["status"],
        input_rows=int(r["input_rows"]),
        output_rows=int(r["output_rows"]),
        duration_seconds=float(r["duration_seconds"]) if r["duration_seconds"] is not None else None,
        error_message=r.get("error_message"),
        created_at=r.get("created_at"),
    )


@router.get("/audit/runs", response_model=AuditRunsResponse)
def get_audit_runs(
    limit: int = Query(default=50, ge=1, le=200),
    stage: Optional[str] = None,
    status: Optional[str] = None,
):
    conditions = []
    if stage is not None:
        if stage not in _ALLOWED_STAGES:
            raise HTTPException(status_code=400, detail=f"stage must be one of {_ALLOWED_STAGES}")
        conditions.append(f"stage = '{stage}'")
    if status is not None:
        if status not in _ALLOWED_STATUSES:
            raise HTTPException(status_code=400, detail=f"status must be one of {_ALLOWED_STATUSES}")
        conditions.append(f"status = '{status}'")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    try:
        rows = fetch_all(f"""
            SELECT
                batch_id, stage, status,
                input_rows, output_rows, duration_seconds, error_message,
                CAST(created_at AS varchar) AS created_at
            FROM nessie.audit.pipeline_batch_runs
            {where}
            ORDER BY created_at DESC
            LIMIT {limit}
        """)
        return AuditRunsResponse(runs=[_to_run(r) for r in rows])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/audit/runs/{batch_id}", response_model=AuditBatchDetail)
def get_audit_batch(batch_id: str):
    if not _BATCH_ID_RE.match(batch_id):
        raise HTTPException(status_code=400, detail="batch_id must be 14-digit numeric string")

    try:
        rows = fetch_all(f"""
            SELECT
                batch_id, stage, status,
                input_rows, output_rows, duration_seconds, error_message,
                CAST(created_at AS varchar) AS created_at
            FROM nessie.audit.pipeline_batch_runs
            WHERE batch_id = '{batch_id}'
            ORDER BY created_at ASC
        """)
        if not rows:
            raise HTTPException(status_code=404, detail="batch_id not found")
        return AuditBatchDetail(batch_id=batch_id, runs=[_to_run(r) for r in rows])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
