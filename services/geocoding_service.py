import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import httpx

from app.settings import settings

log = logging.getLogger("ingest.geocoder")
if not log.handlers:
    logging.basicConfig(level=logging.INFO)
log.setLevel(logging.INFO)


@dataclass
class GeocodeResult:
    display_name: Optional[str]
    street: Optional[str]
    house_number: Optional[str]
    city: Optional[str]
    locality: Optional[str]
    region: Optional[str]
    postcode: Optional[str]
    country: Optional[str]


class ReverseGeocoder:
    def __init__(self) -> None:
        headers = {
            "User-Agent": settings.GEOCODER_USER_AGENT,
        }
        if settings.GEOCODER_EMAIL:
            headers["From"] = settings.GEOCODER_EMAIL
        self._client = httpx.Client(
            timeout=settings.GEOCODER_TIMEOUT_S,
            headers=headers,
        )
        self._cache_precision = settings.GEOCODER_CACHE_PRECISION
        self._cache_ttl_hours = settings.GEOCODER_CACHE_TTL_HOURS

    def lookup(self, cur, lat: float, lon: float) -> Optional[GeocodeResult]:
        key_lat = round(lat, self._cache_precision)
        key_lon = round(lon, self._cache_precision)

        cached = self._lookup_cache(cur, key_lat, key_lon)
        if cached:
            log.debug("reverse geocode cache hit lat=%s lon=%s", lat, lon)
            return cached

        try:
            log.info("reverse geocode fetch lat=%s lon=%s", lat, lon)
            result = self._fetch_remote(lat, lon)
        except Exception as exc:  # pylint: disable=broad-except
            log.warning("reverse geocode failed lat=%s lon=%s err=%s", lat, lon, exc)
            return None

        if result:
            self._persist_cache(cur, key_lat, key_lon, lat, lon, result)
        return result

    def _lookup_cache(self, cur, key_lat: float, key_lon: float) -> Optional[GeocodeResult]:
        cur.execute(
            """
            SELECT payload, fetched_at
            FROM reverse_geocode_cache
            WHERE key_lat=%s AND key_lon=%s
            ORDER BY fetched_at DESC
            LIMIT 1
            """,
            (key_lat, key_lon),
        )
        row = cur.fetchone()
        if not row:
            return None
        fetched_at = row["fetched_at"]
        if (
            self._cache_ttl_hours > 0
            and fetched_at is not None
            and (datetime.now(timezone.utc) - fetched_at).total_seconds()
            > self._cache_ttl_hours * 3600
        ):
            return None
        payload = row["payload"]
        if not isinstance(payload, dict):
            return None
        return self._parse_payload(payload)

    def _persist_cache(
        self,
        cur,
        key_lat: float,
        key_lon: float,
        lat: float,
        lon: float,
        result: GeocodeResult,
    ) -> None:
        payload: Dict[str, Any] = {
            "display_name": result.display_name,
            "street": result.street,
            "house_number": result.house_number,
            "city": result.city,
            "locality": result.locality,
            "region": result.region,
            "postcode": result.postcode,
            "country": result.country,
        }
        cur.execute(
            """
            INSERT INTO reverse_geocode_cache(key_lat, key_lon, lat, lon, payload, fetched_at)
            VALUES (%s, %s, %s, %s, %s, now())
            ON CONFLICT (key_lat, key_lon) DO UPDATE
            SET lat=EXCLUDED.lat,
                lon=EXCLUDED.lon,
                payload=EXCLUDED.payload,
                fetched_at=EXCLUDED.fetched_at
            """,
            (key_lat, key_lon, lat, lon, json.dumps(payload)),
        )

    def _fetch_remote(self, lat: float, lon: float) -> Optional[GeocodeResult]:
        params = {
            "lat": lat,
            "lon": lon,
            "format": "jsonv2",
            "addressdetails": 1,
            "accept-language": settings.GEOCODER_LANGUAGE,
        }
        if settings.GEOCODER_EMAIL:
            params["email"] = settings.GEOCODER_EMAIL

        response = self._client.get(settings.GEOCODER_URL, params=params)
        response.raise_for_status()
        data = response.json()
        if not data:
            log.warning("reverse geocode empty response lat=%s lon=%s", lat, lon)
            return None
        return self._parse_payload_from_remote(data)

    def _parse_payload_from_remote(self, data: Dict[str, Any]) -> Optional[GeocodeResult]:
        address = data.get("address") or {}
        if not isinstance(address, dict):
            address = {}

        street = (
            address.get("road")
            or address.get("pedestrian")
            or address.get("path")
            or address.get("residential")
            or address.get("street")
            or address.get("cycleway")
        )
        house_number = address.get("house_number")
        city = (
            address.get("city")
            or address.get("town")
            or address.get("municipality")
            or address.get("village")
            or address.get("county")
        )
        locality = address.get("suburb") or address.get("neighbourhood")
        region = address.get("state") or address.get("state_district")
        postcode = address.get("postcode")
        country = address.get("country")
        display_name = data.get("display_name")

        return GeocodeResult(
            display_name=display_name,
            street=street,
            house_number=house_number,
            city=city,
            locality=locality,
            region=region,
            postcode=postcode,
            country=country,
        )

    def _parse_payload(self, payload: Dict[str, Any]) -> GeocodeResult:
        return GeocodeResult(
            display_name=payload.get("display_name"),
            street=payload.get("street"),
            house_number=payload.get("house_number"),
            city=payload.get("city"),
            locality=payload.get("locality"),
            region=payload.get("region"),
            postcode=payload.get("postcode"),
            country=payload.get("country"),
        )


reverse_geocoder = ReverseGeocoder()
