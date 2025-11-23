import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Dict, Optional

from psycopg import sql

from app.db import get_conn
from app.services.tiles_runner import RunOptions, TileResult, TilesWorkerRunner
from app.settings import settings

log = logging.getLogger("worker.service")


@dataclass
class WorkerRunConfig:
    region_iso: Optional[str] = None
    max_tiles: Optional[int] = None
    batch: Optional[int] = None
    respect_schedule: Optional[bool] = None
    force: bool = False
    requested_by: Optional[str] = None


class WorkerRunService:
    def __init__(self) -> None:
        self._active_tasks: Dict[int, asyncio.Task] = {}
        self._cancel_flags: Dict[int, asyncio.Event] = {}
        self._lock = asyncio.Lock()

    async def create_run(self, config: WorkerRunConfig) -> Dict:
        effective_config = WorkerRunConfig(
            region_iso=config.region_iso,
            max_tiles=config.max_tiles if config.max_tiles is not None else settings.WORKER_RELEASE_BATCH,
            batch=config.batch,
            respect_schedule=config.respect_schedule,
            force=config.force,
            requested_by=config.requested_by,
        )
        run_id = await asyncio.to_thread(self._insert_run, effective_config)
        await self._start_background(run_id, effective_config)
        return await self.get_run(run_id)

    async def list_runs(self, limit: int = 20) -> Dict:
        return await asyncio.to_thread(self._fetch_runs, limit)

    async def get_run(self, run_id: int) -> Optional[Dict]:
        return await asyncio.to_thread(self._fetch_run, run_id)

    async def cancel_run(self, run_id: int) -> Dict:
        await asyncio.to_thread(self._mark_cancel_requested, run_id)
        async with self._lock:
            event = self._cancel_flags.get(run_id)
            if event:
                event.set()
        await asyncio.to_thread(self._append_log, run_id, "INFO", "cancel requested")
        return await self.get_run(run_id)

    async def _start_background(self, run_id: int, config: WorkerRunConfig) -> None:
        async with self._lock:
            if run_id in self._active_tasks:
                return
            loop = asyncio.get_running_loop()
            cancel_event = asyncio.Event()
            self._cancel_flags[run_id] = cancel_event
            task = loop.create_task(self._execute_run(run_id, config, cancel_event))
            self._active_tasks[run_id] = task
            task.add_done_callback(lambda _: asyncio.create_task(self._cleanup(run_id)))

    async def _cleanup(self, run_id: int) -> None:
        async with self._lock:
            self._active_tasks.pop(run_id, None)
            self._cancel_flags.pop(run_id, None)

    async def _execute_run(self, run_id: int, config: WorkerRunConfig, cancel_event: asyncio.Event) -> None:
        def run_job():
            runner = TilesWorkerRunner()
            options = runner.build_default_options()
            if config.batch:
                options.batch = config.batch
            if config.max_tiles is not None:
                options.max_tiles = config.max_tiles
            if config.region_iso is not None:
                options.region_iso = config.region_iso
            if config.respect_schedule is not None:
                options.respect_schedule = config.respect_schedule
            if config.force:
                options.respect_schedule = False

            self._mark_run_started(run_id, options)

            def on_start(result: TileResult):
                self._record_tile_start(run_id, result)

            def on_complete(result: TileResult):
                self._record_tile_complete(run_id, result)

            try:
                summary = runner.run(
                    options=options,
                    on_tile_start=on_start,
                    on_tile_complete=on_complete,
                    exit_when_idle=True,
                    stop_condition=cancel_event.is_set,
                )
                status = "cancelled" if cancel_event.is_set() else ("error" if summary.errored > 0 else "done")
                self._mark_run_finished(run_id, status, None, summary)
            except Exception as exc:  # pylint: disable=broad-except
                log.exception("worker run failed id=%s err=%s", run_id, exc)
                self._mark_run_finished(run_id, "error", str(exc), None)

        await asyncio.to_thread(run_job)

    def _insert_run(self, config: WorkerRunConfig) -> int:
        base_metadata = {
            "batch": config.batch,
            "max_tiles": config.max_tiles,
            "force": config.force,
        }
        if config.respect_schedule is not None:
            base_metadata["respect_schedule"] = config.respect_schedule
        base_metadata = {k: v for k, v in base_metadata.items() if v is not None and v != {}}
        with get_conn() as cur:
            cur.execute(
                """
                INSERT INTO worker_run(status, region_iso, max_tiles, respect_schedule, force, metadata, requested_by)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
                """,
                (
                    "pending",
                    config.region_iso,
                    config.max_tiles,
                    True if config.respect_schedule is None else config.respect_schedule,
                    config.force,
                    json.dumps(base_metadata),
                    config.requested_by,
                ),
            )
            row = cur.fetchone()
            run_id = row["id"]
        self._append_log(run_id, "INFO", "run created", {"config": base_metadata})
        return run_id

    def _mark_run_started(self, run_id: int, options: RunOptions) -> None:
        with get_conn() as cur:
            cur.execute(
                """
                UPDATE worker_run
                SET status='running',
                    started_at=now(),
                    metadata = metadata || %s::jsonb
                WHERE id=%s
                """,
                (
                    json.dumps(
                        {
                            "batch": options.batch,
                            "max_tiles": options.max_tiles,
                            "region": options.region_iso,
                            "respect_schedule": options.respect_schedule,
                        }
                    ),
                    run_id,
                ),
            )
        self._append_log(run_id, "INFO", "run started")

    def _mark_run_finished(self, run_id: int, status: str, error_msg: Optional[str], summary) -> None:
        with get_conn() as cur:
            tiles_total = summary.processed if summary else None
            tiles_done = summary.done if summary else None
            tiles_error = summary.errored if summary else None
            assignments = [
                sql.SQL("status = %s"),
                sql.SQL("finished_at = now()"),
                sql.SQL("cancel_requested = FALSE"),
            ]
            params = [status]
            if error_msg:
                assignments.append(sql.SQL("error_msg = %s"))
                params.append(error_msg)
            if tiles_total is not None:
                assignments.append(sql.SQL("tiles_total = %s"))
                params.append(tiles_total)
            if tiles_done is not None:
                assignments.append(sql.SQL("tiles_done = %s"))
                params.append(tiles_done)
            if tiles_error is not None:
                assignments.append(sql.SQL("tiles_error = %s"))
                params.append(tiles_error)

            params.append(run_id)
            query = sql.SQL("UPDATE worker_run SET {} WHERE id=%s").format(sql.SQL(", ").join(assignments))
            cur.execute(query, params)

        message = {"status": status}
        if summary:
            message["summary"] = {
                "processed": summary.processed,
                "done": summary.done,
                "errored": summary.errored,
            }
        if error_msg:
            message["error"] = error_msg
        self._append_log(run_id, "INFO", "run finished", message)

    def _mark_cancel_requested(self, run_id: int) -> None:
        with get_conn() as cur:
            cur.execute(
                """
                UPDATE worker_run
                SET cancel_requested=TRUE
                WHERE id=%s AND status IN ('pending','running')
                """,
                (run_id,),
            )

    def _record_tile_start(self, run_id: int, result: TileResult) -> None:
        with get_conn() as cur:
            cur.execute(
                """
                INSERT INTO worker_run_tile(run_id,tile_id,status,started_at)
                VALUES (%s,%s,'running',now())
                ON CONFLICT DO NOTHING
                """,
                (run_id, result.tile_id),
            )
            inserted = cur.rowcount == 1
            cur.execute(
                """
                UPDATE worker_run_tile
                SET status='running',
                    started_at=now(),
                    finished_at=NULL,
                    error_msg=NULL
                WHERE run_id=%s AND tile_id=%s
                """,
                (run_id, result.tile_id),
            )
            if inserted:
                cur.execute(
                    """
                    UPDATE worker_run
                    SET tiles_total = tiles_total + 1
                    WHERE id=%s
                    """,
                    (run_id,),
                )

    def _record_tile_complete(self, run_id: int, result: TileResult) -> None:
        with get_conn() as cur:
            cur.execute(
                """
                UPDATE worker_run_tile
                SET status=%s,
                    finished_at=now(),
                    error_msg=%s,
                    elements=%s,
                    venues_inserted=%s,
                    venues_updated=%s,
                    courts_inserted=%s,
                    courts_updated=%s
                WHERE run_id=%s AND tile_id=%s
                """,
                (
                    result.status,
                    result.error,
                    result.elements,
                    result.venues_inserted,
                    result.venues_updated,
                    result.courts_inserted,
                    result.courts_updated,
                    run_id,
                    result.tile_id,
                ),
            )
            cur.execute(
                """
                UPDATE worker_run
                SET tiles_done = tiles_done + CASE WHEN %s = 'done' THEN 1 ELSE 0 END,
                    tiles_error = tiles_error + CASE WHEN %s = 'error' THEN 1 ELSE 0 END
                WHERE id=%s
                """,
                (result.status, result.status, run_id),
            )

    def _fetch_run(self, run_id: int) -> Optional[Dict]:
        with get_conn() as cur:
            cur.execute("SELECT * FROM worker_run WHERE id=%s", (run_id,))
            run = cur.fetchone()
            if not run:
                return None
            cur.execute(
                """
                SELECT tile_id, status, error_msg, elements, venues_inserted, venues_updated,
                       courts_inserted, courts_updated, started_at, finished_at
                FROM worker_run_tile
                WHERE run_id=%s
                ORDER BY finished_at DESC NULLS LAST, started_at DESC
                LIMIT 50
                """,
                (run_id,),
            )
            tiles = cur.fetchall()
            cur.execute(
                """
                SELECT id, level, message, context, created_at
                FROM worker_run_log
                WHERE run_id=%s
                ORDER BY created_at DESC
                LIMIT 50
                """,
                (run_id,),
            )
            logs = cur.fetchall()
        return {
            "run": dict(run),
            "tiles": [dict(t) for t in tiles],
            "logs": [dict(l) for l in logs],
        }

    def _fetch_runs(self, limit: int) -> Dict:
        with get_conn() as cur:
            cur.execute(
                """
                SELECT *
                FROM worker_run
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
        return {"runs": [dict(r) for r in rows]}

    def _append_log(self, run_id: int, level: str, message: str, context: Optional[Dict] = None) -> None:
        with get_conn() as cur:
            cur.execute(
                """
                INSERT INTO worker_run_log(run_id, level, message, context)
                VALUES (%s,%s,%s,%s)
                """,
                (run_id, level, message, json.dumps(context or {})),
            )
