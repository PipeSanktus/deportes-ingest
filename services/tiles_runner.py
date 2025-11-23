import logging
import os
import random
import time
import traceback
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import requests
import json as pyjson

from app.settings import settings
from app.services.geocoding_service import GeocodeResult, reverse_geocoder


@dataclass
class TileResult:
    tile_id: int
    z: int
    x: int
    y: int
    region_iso: Optional[str] = None
    elements: int = 0
    venues_inserted: int = 0
    venues_updated: int = 0
    courts_inserted: int = 0
    courts_updated: int = 0
    status: str = "pending"
    error: Optional[str] = None


@dataclass
class RunOptions:
    batch: int
    max_tiles: Optional[int] = None
    region_iso: Optional[str] = None
    respect_schedule: bool = True
    idle_sleep_s: float = 10.0


@dataclass
class RunSummary:
    processed: int = 0
    done: int = 0
    errored: int = 0
    results: List[TileResult] = field(default_factory=list)


SQL_ENSURE_OSM = """
INSERT INTO source_system(code,name) VALUES('osm','OpenStreetMap/Overpass')
ON CONFLICT (code) DO NOTHING;
"""
SQL_GET_OSM_ID = "SELECT id FROM source_system WHERE code='osm' LIMIT 1;"
SQL_LEASE_TEMPLATE = """
WITH take AS (
  SELECT t.id, t.z, t.x, t.y
  FROM public.tile t
  WHERE
    {status_clause}
    AND (%s IS NULL OR t.region_iso = %s)
  ORDER BY COALESCE(t.next_crawl_at, to_timestamp(0)) ASC, t.id ASC
  LIMIT %s
  FOR UPDATE SKIP LOCKED
)
UPDATE public.tile t
SET status='running',
    retries = CASE WHEN t.status='running' THEN t.retries ELSE t.retries+1 END,
    updated_at=now()
FROM take
WHERE t.id = take.id
RETURNING
  take.id, take.z, take.x, take.y,
  ST_XMin(ST_Transform(ST_TileEnvelope(take.z,take.x,take.y),4326)) AS minx,
  ST_YMin(ST_Transform(ST_TileEnvelope(take.z,take.x,take.y),4326)) AS miny,
  ST_XMax(ST_Transform(ST_TileEnvelope(take.z,take.x,take.y),4326)) AS maxx,
  ST_YMax(ST_Transform(ST_TileEnvelope(take.z,take.x,take.y),4326)) AS maxy,
  t.region_iso;
"""
SQL_HEARTBEAT = "UPDATE tile SET updated_at=now() WHERE id=%s;"
SQL_MARK_DONE = """
UPDATE tile
SET status='done',
    last_crawl_at=now(),
    next_crawl_at=now() + (%s || ' minutes')::interval,
    error_msg=NULL,
    updated_at=now()
WHERE id=%s;
"""
SQL_MARK_ERR = """
UPDATE tile
SET status=CASE WHEN retries+1 >= %s THEN 'error' ELSE 'idle' END,
    retries=retries+1,
    last_crawl_at=now(),
    next_crawl_at=now() + (%s || ' minutes')::interval,
    error_msg=left(%s,2000),
    updated_at=now()
WHERE id=%s;
"""
SQL_REAPER = """
UPDATE tile SET status='idle', updated_at=now()
WHERE status='running'
  AND updated_at < now() - make_interval(secs => %s)
RETURNING id;
"""

OVERPASS_QUERY_BBOX = """
[out:json][timeout:25];
(
  nwr["leisure"="pitch"]({s},{w},{n},{e});
  nwr["leisure"="track"]({s},{w},{n},{e});
  nwr["leisure"="recreation_ground"]({s},{w},{n},{e});
  nwr["leisure"="sports_centre"]({s},{w},{n},{e});
  nwr["amenity"="stadium"]({s},{w},{n},{e});
  nwr["sport"]({s},{w},{n},{e});
);
out body center;
""".format


def pt_from_el(el: Dict) -> Optional[Tuple[float, float]]:
    c = el.get("center")
    if isinstance(c, dict):
        return c.get("lon"), c.get("lat")
    g = el.get("geometry") or []
    if not g:
        return None
    lon = sum(p.get("lon", 0.0) for p in g) / len(g)
    lat = sum(p.get("lat", 0.0) for p in g) / len(g)
    return lon, lat


def sports_from_tags(tags: Dict) -> List[str]:
    s = tags.get("sport")
    return [t.strip().lower() for t in s.split(";") if t.strip()] if s else []


def addr_tuple(tags: Dict):
    return (
        tags.get("addr:city"),
        tags.get("addr:street"),
        tags.get("addr:housenumber"),
        tags.get("addr:postcode"),
    )


def find_or_create_address(cur, tags: Dict):
    city, street, number, pc = addr_tuple(tags)
    if not any([city, street, number, pc]):
        return None
    cur.execute(
        """
      SELECT id FROM address
      WHERE city IS NOT DISTINCT FROM %s
        AND street IS NOT DISTINCT FROM %s
        AND number IS NOT DISTINCT FROM %s
        AND postcode IS NOT DISTINCT FROM %s
      LIMIT 1
    """,
        (city, street, number, pc),
    )
    r = cur.fetchone()
    if r:
        return r["id"]
    cur.execute(
        """INSERT INTO address(city,street,number,postcode)
           VALUES(%s,%s,%s,%s) RETURNING id""",
        (city, street, number, pc),
    )
    return cur.fetchone()["id"]


def resolve_address(cur, tags: Dict, lon: float, lat: float) -> Tuple[Optional[int], Optional[str], Optional[GeocodeResult]]:
    geocode = None
    try:
        geocode = reverse_geocoder.lookup(cur, lat, lon)
    except Exception as exc:  # pylint: disable=broad-except
        logging.getLogger("worker.geocode").warning(
            "reverse geocode failed lon=%s lat=%s err=%s", lon, lat, exc
        )
    if geocode:
        if geocode.street:
            tags["addr:street"] = geocode.street
        if geocode.house_number:
            tags["addr:housenumber"] = geocode.house_number
        if geocode.city or geocode.locality or geocode.region:
            tags["addr:city"] = geocode.city or geocode.locality or geocode.region
        if geocode.postcode:
            tags["addr:postcode"] = geocode.postcode
        if geocode.display_name:
            tags["addr:full"] = geocode.display_name

    city, street, number, pc = addr_tuple(tags)
    addr_id = find_or_create_address(cur, tags)
    address_text = tags.get("addr:full")
    if not address_text and geocode and geocode.display_name:
        address_text = geocode.display_name

    return addr_id, address_text, geocode


def ensure_sport_links(cur, codes: List[str], venue_id=None, court_id=None):
    for code in codes:
        cur.execute(
            "INSERT INTO sport(code,name) VALUES(%s,%s) ON CONFLICT (code) DO NOTHING",
            (code, code.title()),
        )
        if venue_id:
            cur.execute(
                """INSERT INTO venue_sport(venue_id,sport_id)
                   SELECT %s, id FROM sport WHERE code=%s
                   ON CONFLICT DO NOTHING""",
                (venue_id, code),
            )
        if court_id:
            cur.execute(
                """INSERT INTO court_sport(court_id,sport_id)
                   SELECT %s, id FROM sport WHERE code=%s
                   ON CONFLICT DO NOTHING""",
                (court_id, code),
            )


def upsert_tags(cur, entity_type: str, entity_id: int, tags: Dict):
    for k, v in (tags or {}).items():
        cur.execute(
            """INSERT INTO entity_tag(entity_type,entity_id,k,v)
               VALUES(%s,%s,%s,%s)
               ON CONFLICT (entity_type,entity_id,k) DO UPDATE SET v=EXCLUDED.v""",
            (entity_type, entity_id, k, str(v)),
        )


def find_or_create_venue(cur, name: str, lon: float, lat: float, tags: Dict):
    cur.execute(
        """
    SELECT id FROM venue
    WHERE similarity(name,%s) > 0.6
      AND ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s,%s),4326)::geography,
            80
          )
    ORDER BY similarity(name,%s) DESC
    LIMIT 1
    """,
        (name, lon, lat, name),
    )
    r = cur.fetchone()
    addr_id, address_text, _ = resolve_address(cur, tags, lon, lat)
    if r:
        vid = r["id"]
        set_parts = ["updated_at=now()"]
        params = []
        if address_text:
            set_parts.append("address = COALESCE(NULLIF(address,''), %s)")
            params.append(address_text)
        if addr_id:
            set_parts.append("address_id = COALESCE(address_id, %s)")
            params.append(addr_id)
        if len(set_parts) > 1:
            params.append(vid)
            cur.execute(f"UPDATE venue SET {', '.join(set_parts)} WHERE id=%s", params)
        return vid, False
    cur.execute(
        """INSERT INTO venue(name,address,geom,address_id)
           VALUES(%s,%s,ST_SetSRID(ST_Point(%s,%s),4326),%s)
           RETURNING id""",
        (name, address_text, lon, lat, addr_id),
    )
    return cur.fetchone()["id"], True


def upsert_court(cur, venue_id: int, name: str, sport: Optional[str], lon: float, lat: float, tags: Dict):
    cur.execute(
        """
    SELECT id FROM court
    WHERE venue_id=%s
      AND COALESCE(sport,'')=COALESCE(%s,'')
      AND ST_DWithin(
            geom::geography,
            ST_SetSRID(ST_Point(%s,%s),4326)::geography,
            40
          )
    LIMIT 1
    """,
        (venue_id, sport, lon, lat),
    )
    r = cur.fetchone()
    if r:
        cid = r["id"]
        cur.execute(
            "UPDATE court SET tags=COALESCE(tags,'{}'::jsonb)||%s::jsonb, updated_at=now() WHERE id=%s",
            (pyjson.dumps(tags), cid),
        )
        return cid, False
    cur.execute(
        """INSERT INTO court(venue_id,name,sport,geom,tags)
           VALUES(%s,%s,%s,ST_SetSRID(ST_Point(%s,%s),4326),%s)
           RETURNING id""",
        (venue_id, name, sport, lon, lat, pyjson.dumps(tags)),
    )
    return cur.fetchone()["id"], True


def upsert_external_ref(cur, entity_type: str, entity_id: int, source_id: int, el: Dict):
    cur.execute(
        """INSERT INTO external_ref(entity_type,entity_id,source_id,source_key,payload)
           VALUES(%s,%s,%s,%s,%s)
           ON CONFLICT (entity_type,source_id,source_key)
           DO UPDATE SET entity_id=EXCLUDED.entity_id, payload=EXCLUDED.payload""",
        (entity_type, entity_id, source_id, f"{el.get('type')}/{el.get('id')}", pyjson.dumps(el)),
    )


class TilesWorkerRunner:
    def __init__(
        self,
        *,
        db_dsn: Optional[str] = None,
        overpass_url: Optional[str] = None,
        session: Optional[requests.Session] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.db_dsn = db_dsn or settings.DATABASE_URL
        self.overpass_url = overpass_url or settings.OVERPASS_URL

        self.batch_default = int(os.getenv("BATCH", getattr(settings, "WORKER_BATCH", 25)))
        self.max_tiles_default = int(os.getenv("MAX_TILES_TOTAL", getattr(settings, "WORKER_MAX", 1_000_000)))
        self.backoff_ok_min = int(os.getenv("BACKOFF_OK_MIN", getattr(settings, "BACKOFF_OK_MIN", 1440)))
        self.backoff_err_min = int(os.getenv("BACKOFF_ERR_MIN", getattr(settings, "BACKOFF_ERR_MIN", 30)))
        self.max_retries = int(os.getenv("MAX_RETRIES", getattr(settings, "MAX_RETRIES", 6)))
        self.min_req_interval_ms = int(getattr(settings, "MIN_REQ_INTERVAL_MS", 900))
        self.jitter_ms = int(getattr(settings, "JITTER_MS", 250))
        self.http_timeout_s = int(getattr(settings, "HTTP_TIMEOUT_S", 60))
        self.reaper_stale_s = int(getattr(settings, "REAPER_STALE_SECS", 600))
        self.user_agent = getattr(settings, "WORKER_USER_AGENT", "sports-app-tiles/1.0")
        self.region_iso_default = os.getenv("REGION_ISO", getattr(settings, "REGION_ISO", None))

        self.logger = logger or logging.getLogger("worker.tiles")
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})

        self._last_req_ms = 0

    def build_default_options(self) -> RunOptions:
        return RunOptions(
            batch=self.batch_default,
            max_tiles=self.max_tiles_default,
            region_iso=self.region_iso_default,
            respect_schedule=True,
            idle_sleep_s=10.0,
        )

    def run(
        self,
        *,
        options: Optional[RunOptions] = None,
        on_tile_start: Optional[Callable[[TileResult], None]] = None,
        on_tile_complete: Optional[Callable[[TileResult], None]] = None,
        exit_when_idle: bool = True,
        stop_condition: Optional[Callable[[], bool]] = None,
    ) -> RunSummary:
        opts = options or self.build_default_options()
        lease_sql = self._get_lease_sql(opts.respect_schedule)
        summary = RunSummary()
        batch = opts.batch or self.batch_default
        limit = opts.max_tiles if opts.max_tiles not in (None, 0) else None
        region_iso = opts.region_iso if opts.region_iso not in ("", None) else None

        self.logger.info(
            "runner start dsn=%s batch=%s max=%s region=%s respect_schedule=%s",
            self.db_dsn,
            batch,
            limit if limit is not None else "inf",
            region_iso or "-",
            opts.respect_schedule,
        )

        with psycopg2.connect(self.db_dsn) as conn:
            conn.autocommit = False
            while True:
                if stop_condition and stop_condition():
                    self.logger.info("runner stop_condition_triggered=1")
                    break

                try:
                    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                        if self.reaper_stale_s:
                            cur.execute(SQL_REAPER, (self.reaper_stale_s,))
                            freed = cur.fetchall()
                            if freed:
                                self.logger.warning("reaper_reset=%d", len(freed))

                        req_batch = batch
                        if limit is not None:
                            remaining = limit - summary.processed
                            if remaining <= 0:
                                self.logger.info("runner limit_reached=%d", limit)
                                return summary
                            req_batch = min(batch, remaining)
                            if req_batch <= 0:
                                self.logger.info("runner lease_skip req_batch<=0 remaining=%d", remaining)
                                return summary
                        cur.execute(lease_sql, (region_iso, region_iso, req_batch))
                        rows = cur.fetchall()
                        if not rows:
                            conn.commit()
                            if exit_when_idle:
                                self.logger.info("runner idle_exit=1")
                                break
                            self.logger.info("runner idle_sleep=%.1fs", opts.idle_sleep_s)
                            time.sleep(opts.idle_sleep_s)
                            continue

                        for row in rows:
                            region_val = None
                            try:
                                if hasattr(row, "keys") and "region_iso" in row.keys():
                                    region_val = row["region_iso"]
                            except Exception:
                                region_val = None

                            result = TileResult(
                                tile_id=row["id"],
                                z=row["z"],
                                x=row["x"],
                                y=row["y"],
                                region_iso=region_val,
                                status="running",
                            )
                            if on_tile_start:
                                try:
                                    on_tile_start(result)
                                except Exception:
                                    self.logger.exception("runner on_tile_start_failed tile_id=%s", result.tile_id)

                            result = self._process_tile_with_retry(conn, row, result)
                            summary.results.append(result)
                            if result.status == "done":
                                summary.done += 1
                            else:
                                summary.errored += 1
                            summary.processed += 1
                            if on_tile_complete:
                                try:
                                    on_tile_complete(result)
                                except Exception:
                                    self.logger.exception("runner on_tile_complete_failed tile_id=%s", result.tile_id)
                            if limit is not None and summary.processed >= limit:
                                self.logger.info("runner max_tiles_reached=%d", summary.processed)
                                return summary
                except Exception as loop_err:
                    conn.rollback()
                    self.logger.error("runner loop_error=%s\n%s", str(loop_err), traceback.format_exc())
                    time.sleep(5)
        return summary

    def _process_tile_with_retry(self, conn, row, result: TileResult) -> TileResult:
        tid = row["id"]
        z, x, y = row["z"], row["x"], row["y"]
        minx, miny, maxx, maxy = row["minx"], row["miny"], row["maxx"], row["maxy"]
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            self.logger.info(
                "processing tile=%s/%s/%s id=%s region=%s",
                z,
                x,
                y,
                tid,
                result.region_iso or "-",
            )
            cur.execute(SQL_HEARTBEAT, (tid,))
            try:
                time.sleep(random.uniform(0, 0.5))
                elems, iv, uv, ic, uc = self._process_tile(cur, row)
                result.elements = elems
                result.venues_inserted = iv
                result.venues_updated = uv
                result.courts_inserted = ic
                result.courts_updated = uc
                changed = (iv + uv + ic + uc) > 0
                next_min = 43200 if elems == 0 else (self.backoff_ok_min if changed else 10080)
                cur.execute(SQL_MARK_DONE, (next_min, tid))
                conn.commit()
                result.status = "done"
                self.logger.info(
                    "done tile=%s/%s/%s id=%s elems=%d venues(+%d/%d) courts(+%d/%d)",
                    z,
                    x,
                    y,
                    tid,
                    elems,
                    iv,
                    uv,
                    ic,
                    uc,
                )
            except Exception as e:
                conn.rollback()
                err = f"{e}\n{traceback.format_exc()}"
                result.status = "error"
                result.error = str(e)
                with conn.cursor() as cur_err:
                    cur_err.execute(SQL_MARK_ERR, (self.max_retries, self.backoff_err_min, err, tid))
                    conn.commit()
                self.logger.error("fail tile=%s/%s/%s id=%s err=%s", z, x, y, tid, str(e))
        return result

    def _process_tile(self, cur, row) -> Tuple[int, int, int, int, int]:
        tid, z, x, y = row["id"], row["z"], row["x"], row["y"]
        minx, miny, maxx, maxy = row["minx"], row["miny"], row["maxx"], row["maxy"]
        if not (minx < maxx and miny < maxy):
            raise ValueError(f"bbox invalido z/x/y={z}/{x}/{y} vals={minx,miny,maxx,maxy}")
        if abs(minx) < 1 and abs(maxx) < 1 and abs(miny) < 1 and abs(maxy) < 1:
            raise ValueError(f"bbox sospechoso cercano a 0: {minx,miny,maxx,maxy}")

        attempt = 0
        while True:
            attempt += 1
            try:
                t0 = time.time()
                data = self._fetch_overpass(minx, miny, maxx, maxy, attempt)
                dt = time.time() - t0
                self.logger.info(
                    "tile=%s/%s/%s overpass_ok=1 elems=%d time=%.2fs",
                    z,
                    x,
                    y,
                    len(data.get("elements", [])),
                    dt,
                )
                break
            except Exception as e:
                back = min(60, 2**min(attempt, 6)) + random.uniform(0, 1)
                self.logger.warning(
                    "tile=%s/%s/%s attempt=%d overpass_error=%s backoff=%.1fs",
                    z,
                    x,
                    y,
                    attempt,
                    str(e),
                    back,
                )
                if attempt >= self.max_retries:
                    raise
                time.sleep(back)

        cur.execute(SQL_ENSURE_OSM)
        cur.execute(SQL_GET_OSM_ID)
        src_id = cur.fetchone()[0]

        iv = uv = ic = uc = 0
        elems = data.get("elements", [])
        for el in elems:
            tags = el.get("tags") or {}
            name = (tags.get("name") or tags.get("official_name") or "Centro Deportivo").strip()
            pt = pt_from_el(el)
            if not pt:
                continue
            lon, lat = pt
            leisure = tags.get("leisure")
            amenity = tags.get("amenity")
            sports = sports_from_tags(tags)

            vid = None
            if leisure == "sports_centre" or amenity == "stadium":
                vid, created = find_or_create_venue(cur, name, lon, lat, tags)
                ensure_sport_links(cur, sports, venue_id=vid)
                upsert_tags(cur, "venue", vid, tags)
                upsert_external_ref(cur, "venue", vid, src_id, el)
                iv += int(created)
                uv += int(not created)

            if leisure == "pitch" or tags.get("sport"):
                if not vid:
                    vid, created = find_or_create_venue(cur, name, lon, lat, tags)
                    iv += int(created)
                    uv += int(not created)
                s = sports[0] if sports else None
                cname = name or (f"Cancha {s.title()}" if s else "Cancha")
                cid, cnew = upsert_court(cur, vid, cname, s, lon, lat, tags)
                ensure_sport_links(cur, sports[:1] if s else [], court_id=cid)
                upsert_tags(cur, "court", cid, tags)
                upsert_external_ref(cur, "court", cid, src_id, el)
                ic += int(cnew)
                uc += int(not cnew)

        return len(elems), iv, uv, ic, uc

    def _fetch_overpass(self, minx: float, miny: float, maxx: float, maxy: float, attempt: int):
        self._rate_limit()
        q = OVERPASS_QUERY_BBOX(s=miny, w=minx, n=maxy, e=maxx)
        self.logger.info("overpass_bbox s=%.6f w=%.6f n=%.6f e=%.6f", miny, minx, maxy, maxx)
        time.sleep(random.uniform(2, 5) * attempt)
        r = self.session.post(self.overpass_url, data={"data": q}, timeout=self.http_timeout_s)
        if r.status_code >= 400:
            self.logger.error("overpass_http_%s body_head=%r", r.status_code, r.text[:300])
            r.raise_for_status()
        data = r.json()
        self.logger.debug("overpass_elements=%d", len(data.get("elements", [])))
        return data

    def _rate_limit(self):
        now = int(time.time() * 1000)
        wait = (self._last_req_ms + self.min_req_interval_ms) - now
        if wait > 0:
            time.sleep(wait / 1000.0)
        self._last_req_ms = int(time.time() * 1000)
        if self.jitter_ms:
            time.sleep(random.uniform(0, self.jitter_ms) / 1000.0)

    @staticmethod
    def _get_lease_sql(respect_schedule: bool) -> str:
        if respect_schedule:
            status_clause = (
                "(t.status IN ('idle','error') "
                "OR (t.status='done' AND t.next_crawl_at IS NOT NULL AND t.next_crawl_at <= now()))"
            )
        else:
            status_clause = "(t.status <> 'running')"
        return SQL_LEASE_TEMPLATE.format(status_clause=status_clause)
