from contextlib import contextmanager
import psycopg
from psycopg.rows import dict_row

from app.settings import settings

@contextmanager
def get_conn():
    with psycopg.connect(settings.DATABASE_URL, row_factory=dict_row, autocommit=False) as conn:
        with conn.cursor() as cur:
            yield cur
        conn.commit()

def ensure_source_osm(cur):
    cur.execute("SELECT id FROM source_system WHERE code=%s", ("osm",))
    row = cur.fetchone()
    if row:
        return row["id"]
    cur.execute("INSERT INTO source_system(code,name) VALUES(%s,%s) RETURNING id", ("osm","OpenStreetMap/Overpass"))
    return cur.fetchone()["id"]


