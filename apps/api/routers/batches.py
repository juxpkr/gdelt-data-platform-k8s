from fastapi import APIRouter, HTTPException
from db.trino import fetch_all
from models.schemas import BatchesResponse, BatchItem

router = APIRouter()


@router.get("/batches", response_model=BatchesResponse)
def get_batches():
    try:
        rows = fetch_all("""
            SELECT
                source_batch_id,
                COUNT(*) AS event_count,
                CAST(MIN(ingested_at) AS varchar) AS first_ingested_at,
                CAST(MAX(ingested_at) AS varchar) AS last_ingested_at
            FROM nessie.bronze.gdelt_events
            GROUP BY source_batch_id
            ORDER BY source_batch_id DESC
            LIMIT 20
        """)
        batches = [
            BatchItem(
                source_batch_id=r["source_batch_id"],
                event_count=int(r["event_count"]),
                first_ingested_at=r["first_ingested_at"],
                last_ingested_at=r["last_ingested_at"],
            )
            for r in rows
        ]
        return BatchesResponse(batches=batches)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
