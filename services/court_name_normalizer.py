import argparse
import json
import logging
import re
from typing import Dict, List, Optional

from app.db import get_conn

log = logging.getLogger("workers.court_name_normalizer")

GENERIC_NAMES = {
    "centro deportivo",
    "centro deportivo municipal",
    "centro de deportes",
    "cancha deportiva",
    "cancha",
    "multicancha",
    "cancha multiple",
    "complejo deportivo",
    "estadio",
}

SPORT_PREFIX = {
    "soccer": "Cancha",
    "futsal": "Cancha",
    "football": "Cancha",
    "basketball": "Cancha",
    "volleyball": "Cancha",
    "hockey": "Cancha",
    "tennis": "Cancha",
    "padel": "Cancha",
    "equestrian": "Club",
    "swimming": "Piscina",
}

NEIGHBOURHOOD_KEYS = (
    "addr:neighbourhood",
    "addr:suburb",
    "addr:hamlet",
    "neighbourhood",
    "suburb",
)


def _simplify(value: Optional[str]) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _beautify(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _is_generic(name: Optional[str]) -> bool:
    simplified = _simplify(name)
    if not simplified:
        return True
    if simplified in GENERIC_NAMES:
        return True
    return False


def _choose_prefix(sport: Optional[str]) -> str:
    sport_key = (sport or "").lower()
    if sport_key in SPORT_PREFIX:
        return SPORT_PREFIX[sport_key]
    return "Cancha"


def _extract_tag(tags: Dict[str, str], key: str) -> Optional[str]:
    value = tags.get(key)
    if value:
        return value
    if ":" in key:
        short = key.split(":", 1)[1]
        return tags.get(short)
    return None


def _build_candidate_name(row: Dict) -> Optional[str]:
    tags = row.get("tags") or {}
    if isinstance(tags, str):
        tags = json.loads(tags)

    street = (
        _extract_tag(tags, "addr:street")
        or row.get("street")
        or _extract_tag(tags, "street")
    )
    neighbourhood = next(
        (tags.get(key) for key in NEIGHBOURHOOD_KEYS if tags.get(key)), None
    )
    city = _extract_tag(tags, "addr:city") or row.get("city")
    description = tags.get("description")
    operator = tags.get("operator")
    venue_name = row.get("venue_name")

    pieces: List[str] = []
    if street:
        pieces.append(street)
    if neighbourhood and neighbourhood.lower() not in _simplify(" ".join(pieces)):
        pieces.append(neighbourhood)
    if not pieces and city:
        pieces.append(city)
    if not pieces and operator:
        pieces.append(operator)
    if not pieces and venue_name and not _is_generic(venue_name):
        pieces.append(venue_name)
    if not pieces and description:
        pieces.append(description.split(",")[0])

    if not pieces:
        return None

    base = _beautify(" - ".join(dict.fromkeys(pieces)))
    if not base:
        return None

    prefix = _choose_prefix(row.get("sport") or tags.get("sport"))
    simplified_base = base.lower()
    if simplified_base.startswith(("cancha", "centro", "club")):
        candidate = base
    else:
        candidate = f"{prefix} {base}"

    return candidate[:90]


def _fetch_courts(limit: Optional[int]) -> List[Dict]:
    query = """
        SELECT
            c.id,
            c.name,
            c.sport,
            c.tags,
            v.name AS venue_name,
            v.address,
            a.city,
            a.street,
            a.number,
            a.postcode
        FROM court c
        LEFT JOIN venue v ON v.id = c.venue_id
        LEFT JOIN address a ON a.id = v.address_id
        ORDER BY c.id
    """
    params = ()
    if limit is not None:
        query += " LIMIT %s"
        params = (limit,)

    with get_conn() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def normalize_courts(limit: Optional[int], apply: bool) -> None:
    rows = _fetch_courts(limit)
    changes: List[Dict] = []
    for row in rows:
        if not _is_generic(row.get("name")):
            continue
        candidate = _build_candidate_name(row)
        if candidate and candidate != row.get("name"):
            changes.append(
                {
                    "id": row["id"],
                    "old": row.get("name"),
                    "new": candidate,
                }
            )

    if not changes:
        log.info("No courts needed renaming")
        return

    log.info("Prepared %s court name updates", len(changes))
    for change in changes[:20]:
        log.info("Court %s: '%s' -> '%s'", change["id"], change["old"], change["new"])
    if len(changes) > 20:
        log.info("...and %s more", len(changes) - 20)

    if not apply:
        log.warning("Dry run mode, no changes applied")
        return

    with get_conn() as cur:
        for change in changes:
            cur.execute(
                "UPDATE court SET name = %s WHERE id = %s",
                (change["new"], change["id"]),
            )
    log.info("Applied %s updates", len(changes))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Normalize court names using address metadata"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the first N courts",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Persist the generated names (default: dry run)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    normalize_courts(limit=args.limit, apply=args.apply)


if __name__ == "__main__":
    main()
