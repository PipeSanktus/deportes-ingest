import json, logging, time, httpx, h3
from app.settings import settings
from app.services.tiles_runner import resolve_address
from app.schemas import OSMIngestRequest
from app.db import get_conn, ensure_source_osm

log = logging.getLogger("ingest.osm")

def _sports_list(tags: dict):
    s = tags.get("sport")
    return [t.strip().lower() for t in s.split(";") if t.strip()] if s else []

def _addr_tuple(tags: dict):
    return (
        tags.get("addr:city"),
        tags.get("addr:street"),
        tags.get("addr:housenumber"),
        tags.get("addr:postcode"),
    )

def _point(el: dict):
    if isinstance(el.get("center"), dict):
        c = el["center"]; return c["lon"], c["lat"]
    geom = el.get("geometry") or []
    if not geom: return None
    lon = sum(p.get("lon",0) for p in geom)/len(geom)
    lat = sum(p.get("lat",0) for p in geom)/len(geom)
    return lon, lat

def _ensure_sports(cur, sports, venue_id=None, court_id=None):
    for code in sports:
        cur.execute("INSERT INTO sport(code,name) VALUES(%s,%s) ON CONFLICT (code) DO NOTHING",
                    (code, code.title()))
        if venue_id:
            cur.execute("""
              INSERT INTO venue_sport(venue_id,sport_id)
              SELECT %s, id FROM sport WHERE code=%s
              ON CONFLICT DO NOTHING
            """,(venue_id, code))
        if court_id:
            cur.execute("""
              INSERT INTO court_sport(court_id,sport_id)
              SELECT %s, id FROM sport WHERE code=%s
              ON CONFLICT DO NOTHING
            """,(court_id, code))

def _upsert_entity_tags(cur, entity_type, entity_id, tags: dict):
    for k,v in (tags or {}).items():
        cur.execute("""
          INSERT INTO entity_tag(entity_type,entity_id,k,v)
          VALUES(%s,%s,%s,%s)
          ON CONFLICT (entity_type,entity_id,k) DO UPDATE SET v=EXCLUDED.v
        """,(entity_type,entity_id,k,str(v)))

def upsert_venue(cur, name, lon, lat, tags):
    h = h3.geo_to_h3(lat, lon, 8)
    cur.execute("""
      SELECT id FROM venue
      WHERE similarity(name,%s) > 0.6
        AND ST_DWithin(geom, ST_SetSRID(ST_Point(%s,%s),4326), 0.08)
      ORDER BY similarity(name,%s) DESC LIMIT 1
    """,(name, lon, lat, name))
    r = cur.fetchone()
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
    cur.execute("""
      INSERT INTO venue(name,address,geom,h3_8,address_id)
      VALUES (%s,%s, ST_SetSRID(ST_Point(%s,%s),4326), %s, %s)
      RETURNING id
    """, (name, address_text, lon, lat, h, addr_id))
    return cur.fetchone()["id"], True

def upsert_court(cur, venue_id, name, sport, lon, lat, tags):
    cur.execute("""
      SELECT id FROM court
      WHERE venue_id=%s AND COALESCE(sport,'')=COALESCE(%s,'')
        AND ST_DWithin(geom, ST_SetSRID(ST_Point(%s,%s),4326), 0.04)
      LIMIT 1
    """,(venue_id, sport, lon, lat))
    r = cur.fetchone()
    if r:
        cid = r["id"]
        cur.execute("UPDATE court SET tags=COALESCE(tags,'{}'::jsonb)||%s::jsonb, updated_at=now() WHERE id=%s",
                    (json.dumps(tags), cid))
        return cid, False
    cur.execute("""
      INSERT INTO court(venue_id,name,sport,geom,tags)
      VALUES (%s,%s,%s, ST_SetSRID(ST_Point(%s,%s),4326), %s)
      RETURNING id
    """,(venue_id, name, sport, lon, lat, json.dumps(tags)))
    return cur.fetchone()["id"], True

def upsert_external_ref(cur, entity_type, entity_id, source_id, el):
    cur.execute("""
      INSERT INTO external_ref(entity_type,entity_id,source_id,source_key,payload)
      VALUES(%s,%s,%s,%s,%s)
      ON CONFLICT (entity_type,source_id,source_key)
      DO UPDATE SET entity_id=EXCLUDED.entity_id, payload=EXCLUDED.payload
    """,(entity_type, entity_id, source_id, f"{el['type']}/{el['id']}", json.dumps(el)))

async def run_osm_ingest(req: OSMIngestRequest):
    t0 = time.time()
    minlon,minlat,maxlon,maxlat = req.bbox
    q = (req.query
         .replace("south", str(minlat))
         .replace("west",  str(minlon))
         .replace("north", str(maxlat))
         .replace("east",  str(maxlon)))
    async with httpx.AsyncClient(timeout=60, headers={"User-Agent":"sports-app/ingest"}) as client:
        r = await client.post(settings.OVERPASS_URL, data={"data": q})
        r.raise_for_status()
        data = r.json()

    ins_v=upd_v=ins_c=upd_c=0
    with get_conn() as cur:
        osm_source_id = ensure_source_osm(cur)
        for el in data.get("elements", []):
            tags = dict(el.get("tags") or {})
            name = (tags.get("name") or tags.get("official_name") or "Centro Deportivo").strip()
            pt = _point(el)
            if not pt: continue
            lon, lat = pt
            leisure = tags.get("leisure"); amenity = tags.get("amenity")
            sports = _sports_list(tags)

            vid=None
            if leisure=="sports_centre" or amenity=="stadium":
                vid, created = upsert_venue(cur, name, lon, lat, tags)
                _ensure_sports(cur, sports, venue_id=vid)
                _upsert_entity_tags(cur, "venue", vid, tags)
                upsert_external_ref(cur, "venue", vid, osm_source_id, el)
                ins_v += int(created); upd_v += int(not created)

            if leisure=="pitch" and sports:
                # asegura venue alrededor si aún no creado
                if not vid:
                    vid, created = upsert_venue(cur, name, lon, lat, tags)
                    ins_v += int(created); upd_v += int(not created)
                cname = name or f"Cancha {sports[0].title()}"
                cid, cnew = upsert_court(cur, vid, cname, sports[0], lon, lat, tags)
                _ensure_sports(cur, sports[:1], court_id=cid)
                _upsert_entity_tags(cur, "court", cid, tags)
                upsert_external_ref(cur, "court", cid, osm_source_id, el)
                ins_c += int(cnew); upd_c += int(not cnew)

    dt = time.time() - t0
    log.info("bbox=%s,%s,%s,%s venues(+%d/%d) courts(+%d/%d) elems=%d time=%.2fs",
             minlon,minlat,maxlon,maxlat, ins_v,upd_v, ins_c,upd_c, len(data.get("elements",[])), dt)
    return {"venues_inserted": ins_v, "venues_updated": upd_v,
            "courts_inserted": ins_c, "courts_updated": upd_c,
            "elements": len(data.get("elements", []))}



