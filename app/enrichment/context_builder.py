import json
import os

import asyncpg

from app.llm import llm_complete

ENRICHMENT_DB_URL = os.environ.get("DATABASE_URL", "")


async def get_entity_seed(entity_id: str, business_key: str = "integritasmrv") -> dict:
    if not ENRICHMENT_DB_URL:
        return {}
    conn = await asyncpg.connect(ENRICHMENT_DB_URL)
    try:
        row = await conn.fetchrow(
            """
            SELECT id::text AS id, entity_type, label, source_system, external_ids
            FROM entities
            WHERE id=$1::uuid AND business_key=$2
            """,
            entity_id,
            business_key,
        )
        return dict(row) if row else {}
    finally:
        await conn.close()


async def get_trusted_attributes(entity_id: str, min_confidence: float = 0.7) -> dict:
    if not ENRICHMENT_DB_URL:
        return {}
    conn = await asyncpg.connect(ENRICHMENT_DB_URL)
    try:
        rows = await conn.fetch(
            """
            SELECT attr_key, attr_value, attr_value_json, confidence
            FROM entity_attributes
            WHERE entity_id=$1 AND confidence >= $2 AND is_trusted=true
            ORDER BY confidence DESC
            """,
            entity_id,
            min_confidence,
        )
        result: dict = {}
        for r in rows:
            result[r["attr_key"]] = (
                r["attr_value_json"]
                if r["attr_value_json"] is not None
                else r["attr_value"]
            )
        return result
    finally:
        await conn.close()


async def generate_search_queries(primary: dict, related_count: int = 0) -> list[str]:
    name = str(primary.get("legal_name") or primary.get("label") or "company").strip()
    domain = str(primary.get("domain") or "").strip()
    bare_domain = domain.replace("https://", "").replace("http://", "").strip("/")

    prompt = f"""Generate 10 targeted search queries for enrichment.
Name: {name or "unknown"}
Domain: {domain or "unknown"}
City: {primary.get("city") or "unknown"}
Industry: {primary.get("industry") or "unknown"}
Known related contacts: {related_count}
Return JSON array only.
"""
    deterministic = [
        f'"{name}" official website',
        f'"{name}" linkedin company',
        f'"{name}" crunchbase',
        f'"{name}" pitchbook',
        f'"{name}" company profile',
        f'"{name}" leadership team',
        f'"{name}" funding',
        f'"{name}" news',
        f'"{name}" contact',
        f'"{name}" headquarters',
    ]
    if bare_domain:
        deterministic.insert(0, bare_domain)
        deterministic.insert(1, f'site:{bare_domain} "about"')

    try:
        raw = await llm_complete(
            "query-generator", prompt, json_mode=True, max_tokens=800
        )
        parsed = json.loads(raw)
        llm_queries: list[str] = []
        if isinstance(parsed, list):
            llm_queries = [str(x) for x in parsed]
        elif isinstance(parsed, dict) and isinstance(parsed.get("queries"), list):
            llm_queries = [str(x) for x in parsed["queries"]]

        merged: list[str] = []
        for q in deterministic + llm_queries:
            query = q.strip()
            if query and query not in merged:
                merged.append(query)
            if len(merged) >= 10:
                break
        if merged:
            return merged
    except Exception:
        pass
    return deterministic[:10]


async def build_context_bundle(entity_id: str, round_number: int = 1, business_key: str = "integritasmrv") -> dict:
    seed = await get_entity_seed(entity_id, business_key)
    primary = await get_trusted_attributes(entity_id, min_confidence=0.7)
    if seed:
        primary.setdefault("label", seed.get("label"))
        if seed.get("entity_type") == "company":
            primary.setdefault("legal_name", seed.get("label"))
        ext = seed.get("external_ids") or {}
        if isinstance(ext, str):
            try:
                ext = json.loads(ext)
            except Exception:
                ext = {}
        if isinstance(ext, dict):
            if ext.get("domain"):
                primary.setdefault("domain", ext.get("domain"))
            if ext.get("company_name"):
                primary.setdefault("legal_name", ext.get("company_name"))
    queries = await generate_search_queries(primary, related_count=0)
    return {
        "entity_id": entity_id,
        "round": round_number,
        "primary": primary,
        "queries": queries,
        "business_key": business_key,
    }
