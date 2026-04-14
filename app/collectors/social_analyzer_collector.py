import json
import os

import httpx

from .base import BaseCollector, RawEvidence


class SocialAnalyzerCollector(BaseCollector):
    name = "social_analyzer_api"
    entity_types = ["contact"]
    required_fields = ["full_name"]
    source_weight = 0.70
    uses_scrapiq = False

    async def collect(self, entity: dict, context: dict) -> list[RawEvidence]:
        primary = context.get("primary", {})
        full_name = (primary.get("full_name") or entity.get("label") or "").strip()
        if not full_name:
            return []

        base = os.environ.get("SOCIAL_ANALYZER_URL", "http://127.0.0.1:9005")
        timeout = httpx.Timeout(25)
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(
                f"{base}/analyze_string",
                data={
                    "string": full_name,
                    "option": "FindUserProflesFast",
                    "uuid": str(entity.get("id", "unknown")),
                },
            )
            resp.raise_for_status()
            payload = resp.json()

        content = json.dumps(payload)[:20000]
        return [
            RawEvidence(
                entity_id=entity["id"],
                collector_name=self.name,
                content_type="json",
                raw_content=content,
                source_url=f"{base}/analyze_string",
                source_weight=self.source_weight,
                round_number=int(context.get("round", 1)),
                business_key=str(context.get("business_key", "integritasmrv")),
            )
        ]
