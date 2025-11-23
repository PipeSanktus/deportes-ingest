from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from app.settings import settings


class OSMIngestRequest(BaseModel):
    bbox: List[float] = Field(..., description="minlon,minlat,maxlon,maxlat")
    # Overpass QL template with tokens: south, west, north, east
    query: str = Field(
        default=(
            "[out:json][timeout:25];"
            "("
            '  way["leisure"="pitch"]["sport"](south,west,north,east);'
            '  relation["leisure"="pitch"]["sport"](south,west,north,east);'
            '  way["leisure"="sports_centre"](south,west,north,east);'
            '  relation["leisure"="sports_centre"](south,west,north,east);'
            ");"
            "out center tags;"
        )
    )


class WorkerRunCreate(BaseModel):
    region_iso: Optional[str] = Field(None, description="Código de región opcional para filtrar tiles")
    max_tiles: Optional[int] = Field(
        default=settings.WORKER_RELEASE_BATCH,
        ge=1,
        description="Tiles a procesar en esta corrida (default controlado por WORKER_RELEASE_BATCH, 100)",
    )
    batch: Optional[int] = Field(None, ge=1, description="Cantidad de tiles a leasar por iteración")
    respect_schedule: Optional[bool] = Field(
        None, description="Respetar next_crawl_at; si es False procesa aunque no estén vencidos"
    )
    force: bool = Field(False, description="Ignora programación y procesa tiles pendientes inmediatamente")
    requested_by: Optional[str] = Field(None, description="Identificador del usuario o sistema que gatilla la corrida")


class WorkerRunTileStatus(BaseModel):
    tile_id: int
    status: str
    error_msg: Optional[str] = None
    elements: Optional[int] = None
    venues_inserted: Optional[int] = None
    venues_updated: Optional[int] = None
    courts_inserted: Optional[int] = None
    courts_updated: Optional[int] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None


class WorkerRunLogEntry(BaseModel):
    id: int
    level: str
    message: str
    context: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class WorkerRunRecord(BaseModel):
    id: int
    status: str
    region_iso: Optional[str] = None
    max_tiles: Optional[int] = None
    respect_schedule: bool
    force: bool
    bbox_override: Optional[str] = None
    tiles_total: int
    tiles_done: int
    tiles_error: int
    error_msg: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    cancel_requested: bool
    requested_by: Optional[str] = None
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None


class WorkerRunDetail(BaseModel):
    run: WorkerRunRecord
    tiles: List[WorkerRunTileStatus] = Field(default_factory=list)
    logs: List[WorkerRunLogEntry] = Field(default_factory=list)


class WorkerRunList(BaseModel):
    runs: List[WorkerRunRecord]


class TileRecord(BaseModel):
    id: int
    z: int
    x: int
    y: int
    status: str
    retries: int
    region_iso: str
    last_crawl_at: Optional[datetime] = None
    next_crawl_at: Optional[datetime] = None
    updated_at: datetime


class TileList(BaseModel):
    tiles: List[TileRecord]
    total: int
