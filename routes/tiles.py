from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query

from app.db import get_conn
from app.schemas import TileList, TileRecord

router = APIRouter(prefix="/tiles", tags=["tiles"])

VALID_STATUSES = {"idle", "running", "done", "error"}


@router.get("", response_model=TileList, summary="Listar tiles por estado")
@router.get("/", response_model=TileList, summary="Listar tiles por estado")
async def list_tiles(
    status: Optional[List[str]] = Query(
        None,
        description="Filtra por uno o más estados (idle|running|done|error). Repetir el parámetro para múltiples valores.",
    ),
    limit: int = Query(100, ge=1, le=1000, description="Cantidad máxima de tiles a retornar"),
    offset: int = Query(0, ge=0, description="Offset para paginar resultados"),
):
    statuses = None
    if status:
        invalid = [s for s in status if s not in VALID_STATUSES]
        if invalid:
            raise HTTPException(status_code=400, detail=f"Estados inválidos: {', '.join(invalid)}")
        statuses = status

    with get_conn() as cur:
        params = []
        filters = []
        if statuses:
            filters.append("status = ANY(%s)")
            params.append(statuses)

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

        count_query = f"SELECT COUNT(*) AS total FROM tile {where_clause}"
        cur.execute(count_query, params)
        total = cur.fetchone()["total"]

        query = (
            "SELECT id, z, x, y, status, retries, region_iso, last_crawl_at, next_crawl_at, updated_at "
            f"FROM tile {where_clause} "
            "ORDER BY updated_at DESC "
            "LIMIT %s OFFSET %s"
        )
        cur.execute(query, (*params, limit, offset))
        rows = cur.fetchall()

    tiles = [
        TileRecord(
            id=row["id"],
            z=row["z"],
            x=row["x"],
            y=row["y"],
            status=row["status"],
            retries=row["retries"],
            region_iso=row["region_iso"],
            last_crawl_at=row["last_crawl_at"],
            next_crawl_at=row["next_crawl_at"],
            updated_at=row["updated_at"],
        )
        for row in rows
    ]

    return TileList(tiles=tiles, total=total)
