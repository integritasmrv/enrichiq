import json
import os

import httpx

from .base import BaseCollector, RawEvidence

OVERPASS_URL = os.environ.get("OVERPASS_URL", "https://overpass-api.de/api/interpreter")
NOMINATIM_SEARCH_URL = os.environ.get(
    "NOMINATIM_SEARCH_URL", "http://217.76.59.199:8180/search"
)
OVERPASS_TIMEOUT_SEC = int(os.environ.get("OVERPASS_TIMEOUT_SEC", "25"))
OVERPASS_RADIUS_METERS = int(os.environ.get("OVERPASS_RADIUS_METERS", "100"))

OVERPASS_QUERY_TEMPLATE = """
[out:json][timeout:{timeout}];
(
  node["name"~"{name}",i](around:{radius},{lat},{lon});
  way["name"~"{name}",i](around:{radius},{lat},{lon});
  relation["name"~"{name}",i](around:{radius},{lat},{lon});
);
out body;
"""


def _company_name(primary: dict, entity: dict) -> str:
    return str(
        primary.get("legal_name") or primary.get("label") or entity.get("label") or ""
    ).strip()


def _to_float(v: object) -> float | None:
    try:
        return float(str(v).strip())
    except Exception:
        return None


async def _fallback_geocode(
    primary: dict, entity: dict
) -> tuple[float | None, float | None]:
    query_parts = [
        _company_name(primary, entity),
        str(primary.get("city") or primary.get("hq_city") or "").strip(),
        str(primary.get("country") or primary.get("hq_country") or "").strip(),
    ]
    query = " ".join([p for p in query_parts if p]).strip()
    if not query:
        return None, None
    params = {
        "q": query,
        "format": "jsonv2",
        "addressdetails": 1,
        "limit": 1,
        "accept-language": "en",
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                NOMINATIM_SEARCH_URL,
                params=params,
                headers={"User-Agent": "IntegritasMRV-IntelligenceBot/1.0"},
            )
            resp.raise_for_status()
            rows = resp.json()
        if not rows:
            return None, None
        hit = rows[0]
        return _to_float(hit.get("lat")), _to_float(hit.get("lon"))
    except Exception:
        return None, None


class OverpassCollector(BaseCollector):
    name = "overpass_lookup"
    entity_types = ["company"]
    required_fields = []
    source_weight = 0.75
    uses_scrapiq = False

    async def collect(self, entity: dict, context: dict) -> list[RawEvidence]:
        primary = context.get("primary", {}) if isinstance(context, dict) else {}
        company_name = _company_name(primary, entity)
        if not company_name:
            return []

        lat = _to_float(primary.get("lat") or primary.get("hq_lat"))
        lng = _to_float(
            primary.get("lng") or primary.get("hq_lng") or primary.get("lon")
        )
        if lat is None or lng is None:
            lat, lng = await _fallback_geocode(primary, entity)
        if lat is None or lng is None:
            return []

        overpass_query = OVERPASS_QUERY_TEMPLATE.format(
            timeout=max(5, OVERPASS_TIMEOUT_SEC),
            name=company_name.replace('"', "").replace("\n", " "),
            radius=max(25, OVERPASS_RADIUS_METERS),
            lat=lat,
            lon=lng,
        )

        try:
            async with httpx.AsyncClient(
                timeout=max(10, OVERPASS_TIMEOUT_SEC + 5)
            ) as client:
                resp = await client.post(
                    OVERPASS_URL,
                    data={"data": overpass_query},
                    headers={"User-Agent": "IntegritasMRV-IntelligenceBot/1.0"},
                )
                resp.raise_for_status()
                payload = resp.json()
        except Exception:
            return []

        elements = payload.get("elements") or []
        results: list[dict] = []
        websites: list[str] = []
        for el in elements:
            tags = el.get("tags") or {}
            phone = (
                tags.get("phone") or tags.get("contact:phone") or tags.get("telephone")
            )
            website = (
                tags.get("website") or tags.get("contact:website") or tags.get("url")
            )
            if not phone and not website:
                continue
            item = {
                "osm_id": el.get("id"),
                "osm_type": el.get("type"),
                "name": tags.get("name"),
                "phone": phone,
                "website": website,
                "addr_street": tags.get("addr:street"),
                "addr_housenumber": tags.get("addr:housenumber"),
                "addr_city": tags.get("addr:city"),
                "addr_postcode": tags.get("addr:postcode"),
                "addr_country": tags.get("addr:country"),
            }
            if website:
                websites.append(str(website))
            results.append(item)

        if not results:
            return []

        structured = {
            "query_name": company_name,
            "lat": lat,
            "lng": lng,
            "results": results,
        }

        return [
            RawEvidence(
                entity_id=str(entity.get("id")),
                collector_name=self.name,
                content_type="json",
                raw_content=json.dumps(structured)[:20000],
                source_url=OVERPASS_URL,
                source_weight=self.source_weight,
                round_number=int(context.get("round", 1)),
            )
        ]
