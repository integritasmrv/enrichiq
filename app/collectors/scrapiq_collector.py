import asyncio
import json
import os

import asyncpg
import httpx

from .base import BaseCollector, RawEvidence

N8N_URL = os.environ.get("N8N_URL", "")
N8N_WEBHOOK_TOKEN = os.environ.get("N8N_WEBHOOK_TOKEN", "")
SCRAPIQ_DISPATCH_URL = os.environ.get("SCRAPIQ_DISPATCH_URL", "")
SCRAPIQ_DISPATCH_TOKEN = os.environ.get("SCRAPIQ_DISPATCH_TOKEN", "")
SCRAPIQ_DB_URL = os.environ.get("SCRAPIQ_DATABASE_URL", "")
SOURCE_SYSTEM = os.environ.get("SOURCE_SYSTEM", "crm-forestfuture")
DEFAULT_BUSINESS_KEY = os.environ.get("BUSINESS_KEY", "integritasmrv")

BUSINESS_KEY_TO_SCRAPIQ = {
    "integritasmrv": "integritas",
    "poweriq": "pwriq",
    "airbnb": "airbnb",
    "private": "private",
    "ev-batteries": "ev-batteries",
}


def _to_scrapiq_business(business_key: str) -> str:
    return BUSINESS_KEY_TO_SCRAPIQ.get(business_key, "integritas")


class ScrapIQWebDiscoveryCollector(BaseCollector):
    name = "scrapiq_web_discovery"
    entity_types = ["company", "contact"]
    required_fields = []
    source_weight = 0.75
    uses_scrapiq = True

    async def collect(self, entity: dict, context: dict) -> list[RawEvidence]:
        dispatch_url = SCRAPIQ_DISPATCH_URL or (
            f"{N8N_URL}/webhook/scrapiq-from-service" if N8N_URL else ""
        )
        if not dispatch_url:
            return []

        token = SCRAPIQ_DISPATCH_TOKEN if SCRAPIQ_DISPATCH_URL else N8N_WEBHOOK_TOKEN
        headers = {"Authorization": f"Bearer {token}"} if token else {}

        primary = context.get("primary", {}) if isinstance(context, dict) else {}
        domain = str(primary.get("domain") or "").strip().lower()
        starting_url = f"https://{domain}" if domain else None
        business_key = str(context.get("business_key", DEFAULT_BUSINESS_KEY))
        scrapiq_business = _to_scrapiq_business(business_key)

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                dispatch_url,
                headers=headers,
                json={
                    "request_type": "web_page_discovery",
                    "initiated_by_service": "enrichment-pipeline",
                    "prompt": "\n".join(context.get("queries", [])[:10])
                    or f"Research {entity.get('label', 'entity')}",
                    "starting_url": starting_url,
                    "search_scope": "full_web",
                    "max_results": 15,
                    "min_relevance_score": 0.5,
                    "summarize_results": True,
                    "llm_alias": "summarizer",
                    "context_entity_type": entity.get("entity_type"),
                    "context_entity_name": entity.get("label"),
                    "context_entity_data": context.get("primary", {}),
                    "source_system": SOURCE_SYSTEM,
                    "request_business": scrapiq_business,
                    "enrichment_entity_id": entity.get("id"),
                    "enrichment_round": int(context.get("round", 1)),
                },
            )
            r.raise_for_status()
            payload = r.json()

        if not isinstance(payload, dict) or "request_id" not in payload:
            return [
                RawEvidence(
                    entity_id=str(entity.get("id")),
                    collector_name=self.name,
                    content_type="json",
                    raw_content=json.dumps(payload)[:20000],
                    source_url=dispatch_url,
                    source_weight=self.source_weight,
                    round_number=int(context.get("round", 1)),
                    business_key=business_key,
                )
            ]

        request_id = int(payload["request_id"])

        rows = await self._poll_results(request_id)
        if not rows:
            return [
                RawEvidence(
                    entity_id=str(entity.get("id")),
                    collector_name=self.name,
                    content_type="json",
                    raw_content=json.dumps(
                        {
                            "status": "no_results",
                            "request_id": request_id,
                            "dispatch_url": dispatch_url,
                            "starting_url": starting_url,
                        }
                    )[:20000],
                    source_url=starting_url,
                    source_weight=self.source_weight,
                    round_number=int(context.get("round", 1)),
                    scrapiq_request_id=request_id,
                    business_key=business_key,
                )
            ]

        out: list[RawEvidence] = []
        for row in rows:
            out.append(
                RawEvidence(
                    entity_id=str(entity.get("id")),
                    collector_name=self.name,
                    content_type="text",
                    raw_content=f"{row.get('page_title', '')}\n{row.get('ai_summary', '')}\n{row.get('found_page_url', '')}",
                    source_url=row.get("found_page_url"),
                    source_weight=float(row.get("relevance_score") or 50) / 100,
                    round_number=int(context.get("round", 1)),
                    scrapiq_request_id=request_id,
                    scrapiq_result_id=int(row["id"]),
                    business_key=business_key,
                )
            )
        return out

    async def _poll_results(
        self, request_id: int, timeout_sec: int = 900
    ) -> list[dict]:
        if not SCRAPIQ_DB_URL:
            return []
        start = asyncio.get_event_loop().time()
        while (asyncio.get_event_loop().time() - start) < timeout_sec:
            conn = await asyncpg.connect(SCRAPIQ_DB_URL)
            try:
                row = await conn.fetchrow(
                    "SELECT status FROM nb_scrapiq_requests WHERE id=$1",
                    request_id,
                )
                if row and row["status"] in ("finished", "failed"):
                    items = await conn.fetch(
                        """
                        SELECT id, found_page_url, page_title, ai_summary, relevance_score
                        FROM nb_scrapiq_result_pages
                        WHERE request_id=$1 AND status='stored'
                        ORDER BY relevance_score DESC
                        """,
                        request_id,
                    )
                    return [dict(i) for i in items]
            finally:
                await conn.close()
            await asyncio.sleep(15)
        return []
