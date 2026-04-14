import os

import asyncpg
from temporalio import activity

from app.activities.extraction import extract_fields_llm as _extract_fields_llm
from app.activities.extraction import (
    evaluate_confidence_gap as _evaluate_confidence_gap,
)
from app.activities.extraction import promote_trusted_fields as _promote_trusted_fields
from app.activities.extraction import write_hubspot as _write_hubspot
from app.collectors import COLLECTOR_REGISTRY
from app.enrichment.context_builder import (
    build_context_bundle as _build_context_bundle,
)
from app.enrichment.context_builder import get_trusted_attributes

DATABASE_URL = os.environ.get("DATABASE_URL", os.environ.get("ENRICHMENT_DB_DSN", ""))


async def _get_conn() -> asyncpg.Connection:
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return await asyncpg.connect(DATABASE_URL)


async def _get_entity(entity_id: str) -> dict | None:
    conn = await _get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT id::text AS id, entity_type, label, source_system, business_key FROM entities WHERE id=$1::uuid",
            entity_id,
        )
        return dict(row) if row else None
    finally:
        await conn.close()


@activity.defn(name="set_entity_status")
async def set_entity_status(entity_id: str, status: str) -> None:
    conn = await _get_conn()
    try:
        await conn.execute(
            "UPDATE entities SET enrichment_status=$2, updated_at=NOW() WHERE id=$1::uuid",
            entity_id,
            status,
        )
    finally:
        await conn.close()


@activity.defn(name="build_context_bundle")
async def build_context_bundle(entity_id: str, round_number: int = 1, business_key: str = "integritasmrv") -> dict:
    return await _build_context_bundle(entity_id, round_number, business_key)


@activity.defn(name="get_eligible_collectors")
async def get_eligible_collectors(entity_id: str) -> list[str]:
    entity = await _get_entity(entity_id)
    if not entity:
        return []
    trusted = await get_trusted_attributes(entity_id, min_confidence=0.7)
    eligible: list[str] = []
    for name, klass in COLLECTOR_REGISTRY.items():
        collector = klass()
        if await collector.can_run(entity, trusted):
            eligible.append(name)
    return eligible


@activity.defn(name="run_collector")
async def run_collector(collector_name: str, entity_id: str, context: dict) -> int:
    klass = COLLECTOR_REGISTRY.get(collector_name)
    if not klass:
        return 0

    entity = await _get_entity(entity_id)
    if not entity:
        return 0

    collector = klass()
    evidence = await collector.collect(entity, context)
    if not evidence:
        return 0

    conn = await _get_conn()
    try:
        inserted = 0
        for ev in evidence:
            await conn.execute(
                """
                INSERT INTO raw_evidence
                  (entity_id, collector_name, source_url, content_type, raw_content,
                   source_weight, round_number, scrapiq_request_id, scrapiq_result_id, business_key)
                VALUES ($1::uuid,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                """,
                ev.entity_id,
                ev.collector_name,
                ev.source_url,
                ev.content_type,
                ev.raw_content,
                ev.source_weight,
                ev.round_number,
                ev.scrapiq_request_id,
                ev.scrapiq_result_id,
                ev.business_key,
            )
            inserted += 1
        return inserted
    finally:
        await conn.close()


@activity.defn(name="extract_fields_llm")
async def extract_fields_llm(entity_id: str, round_number: int, business_key: str = "integritasmrv") -> int:
    return await _extract_fields_llm(entity_id, round_number, business_key)


@activity.defn(name="promote_trusted_fields")
async def promote_trusted_fields(entity_id: str) -> int:
    return await _promote_trusted_fields(entity_id)


@activity.defn(name="evaluate_confidence_gap")
async def evaluate_confidence_gap(entity_id: str, round_number: int) -> dict:
    return await _evaluate_confidence_gap(entity_id, round_number)


@activity.defn(name="write_hubspot")
async def write_hubspot(entity_id: str, source_system: str) -> dict:
    return await _write_hubspot(entity_id, source_system)
