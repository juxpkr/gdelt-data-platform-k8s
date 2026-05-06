import os
import trino


def _connect():
    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "trino.gdelt.svc.cluster.local"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "gdelt-api"),
        http_scheme="http",
    )


def fetch_all(sql: str) -> list[dict]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(sql)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [dict(zip(columns, row)) for row in rows]


def fetch_one(sql: str) -> dict | None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(sql)
    columns = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row is None:
        return None
    return dict(zip(columns, row))
