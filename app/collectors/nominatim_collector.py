import json
import os

import httpx

from .base import BaseCollector, RawEvidence

NOMINATIM_SEARCH_URL = os.environ.get(
    "NOMINATIM_SEARCH_URL", "http://217.76.59.199:8180/search"
)
NOMINATIM_ACCEPT_LANGUAGE = os.environ.get("NOMINATIM_ACCEPT_LANGUAGE", "en")


def _build_nominatim_query(primary: dict, entity: dict) -> str:
    name = str(
        primary.get("legal_name") or primary.get("label") or entity.get("label") or ""
    ).strip()
    city = str(primary.get("city") or primary.get("hq_city") or "").strip()
    country = str(primary.get("country") or primary.get("hq_country") or "").strip()
    parts = [p for p in [name, city, country] if p]
    return " ".join(parts).strip()


class NominatimCollector(BaseCollector):
    name = "nominatim_geocode"
    entity_types = ["company"]
    required_fields = []
    source_weight = 0.95
    uses_scrapiq = False

    async def collect(self, entity: dict, context: dict) -> list[RawEvidence]:
        primary = context.get("primary", {}) if isinstance(context, dict) else {}
        query = _build_nominatim_query(primary, entity)
        if not query:
            return []

        params = {
            "q": query,
            "format": "jsonv2",
            "addressdetails": 1,
            "limit": 1,
            "accept-language": NOMINATIM_ACCEPT_LANGUAGE,
        }

        try:
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.get(
                    NOMINATIM_SEARCH_URL,
                    params=params,
                    headers={"User-Agent": "IntegritasMRV-IntelligenceBot/1.0"},
                )
                resp.raise_for_status()
                rows = resp.json()
        except Exception:
            return []

        if not rows:
            return []

        hit = rows[0] if isinstance(rows, list) else {}
        addr = hit.get("address") or {}

        structured = {
            "query": query,
            "nominatim_display_name": hit.get("display_name"),
            "lat": str(hit.get("lat") or "").strip(),
            "lng": str(hit.get("lon") or "").strip(),
            "address": {
                "road": addr.get("road") or "",
                "house_number": addr.get("house_number") or "",
                "city": addr.get("city")
                or addr.get("town")
                or addr.get("village")
                or addr.get("municipality")
                or "",
                "postcode": addr.get("postcode") or "",
                "country": addr.get("country") or "",
                "country_code": str(addr.get("country_code") or "").lower(),
            },
            "osm_type": hit.get("osm_type"),
            "osm_id": hit.get("osm_id"),
            "place_rank": hit.get("place_rank"),
        }

        return [
            RawEvidence(
                entity_id=str(entity.get("id")),
                collector_name=self.name,
                content_type="json",
                raw_content=json.dumps(structured)[:20000],
                source_url=NOMINATIM_SEARCH_URL,
                source_weight=self.source_weight,
                round_number=int(context.get("round", 1)),
            )
        ]
