#!/usr/bin/env python3
import logging
import os
import sys
from typing import Optional

from app.services.tiles_runner import RunOptions, TilesWorkerRunner

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)


def _build_options_from_env(runner: TilesWorkerRunner) -> RunOptions:
    opts = runner.build_default_options()
    batch_env = os.getenv("BATCH")
    max_tiles_env = os.getenv("MAX_TILES_TOTAL")
    region_env = os.getenv("REGION_ISO")
    respect_schedule_env = os.getenv("RESPECT_SCHEDULE")

    if batch_env:
        try:
            opts.batch = int(batch_env)
        except ValueError:
            logging.getLogger("worker.tiles").warning("invalid BATCH env=%s", batch_env)
    if max_tiles_env:
        try:
            opts.max_tiles = int(max_tiles_env)
        except ValueError:
            logging.getLogger("worker.tiles").warning("invalid MAX_TILES_TOTAL env=%s", max_tiles_env)
    if region_env not in (None, ""):
        opts.region_iso = region_env
    if respect_schedule_env is not None:
        opts.respect_schedule = respect_schedule_env.lower() not in ("0", "false", "no")
    return opts


def main(exit_when_idle: Optional[bool] = None):
    runner = TilesWorkerRunner()
    options = _build_options_from_env(runner)
    should_exit = exit_when_idle if exit_when_idle is not None else False
    runner.run(options=options, exit_when_idle=should_exit)


if __name__ == "__main__":
    main()
