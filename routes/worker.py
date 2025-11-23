from fastapi import APIRouter, HTTPException, Query

from app.schemas import (
    WorkerRunCreate,
    WorkerRunDetail,
    WorkerRunList,
    WorkerRunLogEntry,
    WorkerRunRecord,
    WorkerRunTileStatus,
)
from app.services.worker_service import WorkerRunConfig, WorkerRunService

router = APIRouter(prefix="/worker", tags=["worker"])

_service = WorkerRunService()


def _to_detail(payload: dict) -> WorkerRunDetail:
    if not payload:
        raise HTTPException(status_code=500, detail="worker run payload vacío")
    run = WorkerRunRecord(**payload["run"])
    tiles = [WorkerRunTileStatus(**t) for t in payload.get("tiles", [])]
    logs = [WorkerRunLogEntry(**l) for l in payload.get("logs", [])]
    return WorkerRunDetail(run=run, tiles=tiles, logs=logs)


@router.post("/runs", response_model=WorkerRunDetail, status_code=202)
async def create_worker_run(request: WorkerRunCreate) -> WorkerRunDetail:
    config = WorkerRunConfig(
        region_iso=request.region_iso,
        max_tiles=request.max_tiles,
        batch=request.batch,
        respect_schedule=request.respect_schedule,
        force=request.force,
        requested_by=request.requested_by,
    )
    data = await _service.create_run(config)
    return _to_detail(data)


@router.get("/runs", response_model=WorkerRunList)
async def list_worker_runs(limit: int = Query(20, ge=1, le=100)) -> WorkerRunList:
    data = await _service.list_runs(limit=limit)
    runs = [WorkerRunRecord(**r) for r in data.get("runs", [])]
    return WorkerRunList(runs=runs)


@router.get("/runs/{run_id}", response_model=WorkerRunDetail)
async def get_worker_run(run_id: int) -> WorkerRunDetail:
    data = await _service.get_run(run_id)
    if not data:
        raise HTTPException(status_code=404, detail="worker_run no encontrado")
    return _to_detail(data)


@router.post("/runs/{run_id}/cancel", response_model=WorkerRunDetail)
async def cancel_worker_run(run_id: int) -> WorkerRunDetail:
    data = await _service.cancel_run(run_id)
    if not data:
        raise HTTPException(status_code=404, detail="worker_run no encontrado")
    return _to_detail(data)
