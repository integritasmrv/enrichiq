import json
import os
import asyncio

import asyncpg
import httpx
from temporalio import activity

SPIDERFOOT_URL = os.environ.get("SPIDERFOOT_URL", "http://127.0.0.1:5001")
SCRAPIQ_DB_URL = os.environ.get("SCRAPIQ_DATABASE_URL", "")

SPIDERFOOT_PAGE_TYPE_MAP = {
    "INTERNET_NAME": "domain",
    "DOMAIN_NAME": "domain",
    "EMAILADDR": "contact_email",
    "PHONE_NUMBER": "contact_phone",
    "LINKEDIN_URL": "social_profile",
    "SOCIAL_MEDIA": "social_profile",
    "WEB_TECHNOLOGY": "tech_stack",
    "GEOINFO": "company_data",
    "COMPANY_NAME": "company_data",
    "LEAKSITE_URL": "adverse_media",
    "DARKWEB_MENTION": "adverse_media",
    "DOCUMENT_URL": "report",
}


@activity.defn
async def run_spiderfoot_scan(request_id: int, target: str) -> list[dict]:
    async with httpx.AsyncClient(base_url=SPIDERFOOT_URL, timeout=30) as client:
        r = await client.post(
            "/scan/new",
            data={
                "scanname": str(request_id),
                "scantarget": target,
                "usecase": "footprint",
                "modulelist": "",
                "typelist": "",
            },
        )
        r.raise_for_status()
        scan_id = r.json().get("id")
        if not scan_id:
            return []
        for _ in range(120):
            sr = await client.get(f"/scan/{scan_id}/status")
            status = (sr.json() or {}).get("status")
            if status in ("FINISHED", "FAILED", "ABORTED"):
                break
            await asyncio.sleep(10)
        rr = await client.get(f"/scan/{scan_id}/results/json")
        rr.raise_for_status()
        data = rr.json()
        return data if isinstance(data, list) else []


@activity.defn
async def store_spiderfoot_results(request_id: int, events: list[dict]) -> int:
    if not SCRAPIQ_DB_URL:
        return 0
    conn = await asyncpg.connect(SCRAPIQ_DB_URL)
    try:
        for event in events:
            event_type = str(event.get("type") or "")
            page_type = SPIDERFOOT_PAGE_TYPE_MAP.get(event_type, "other")
            found = str(event.get("data") or "")
            if not found:
                continue
            await conn.execute(
                """
                INSERT INTO nb_scrapiq_result_pages
                  (request_id, found_page_url, page_type, ai_summary, relevance_score, discovered_via, status)
                VALUES ($1,$2,$3,$4,85.0,'spiderfoot','stored')
                """,
                request_id,
                found,
                page_type,
                json.dumps(event)[:1000],
            )
        await conn.execute(
            """
            UPDATE nb_scrapiq_requests
            SET status='finished', results_processed=$2, finished_at=NOW()
            WHERE id=$1
            """,
            request_id,
            len(events),
        )
        return len(events)
    finally:
        await conn.close()
