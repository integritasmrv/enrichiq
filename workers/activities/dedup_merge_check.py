import asyncpg
from typing import Literal


CRM_CONFIGS = {
    "integritasmrv": {
        "host": "10.0.13.2",
        "port": 5432,
        "db": "integritasmrv_crm",
        "user": "integritasmrv_crm_user",
        "password": "Int3gr1t@smrv_S3cure_P@ssw0rd_2026",
    },
    "poweriq": {
        "host": "10.0.14.2",
        "port": 5432,
        "db": "poweriq_crm",
        "user": "poweriq_crm_user",
        "password": "P0w3r1Q_CRM_S3cur3_P@ss_2026",
    },
}


async def dedup_merge_check(
    entity_id: int,
    external_ids: dict,
    entity_attributes: dict,
    target_crm: str = "integritasmrv",
    table: str = "nb_crm_contacts",
) -> dict:
    config = CRM_CONFIGS.get(target_crm)
    if not config:
        raise ValueError(f"Unknown CRM: {target_crm}")

    conn = await asyncpg.connect(
        host=config["host"],
        port=config["port"],
        database=config["db"],
        user=config["user"],
        password=config["password"],
    )

    try:
        matches = []

        if external_ids.get("hubspot_id"):
            row = await conn.fetchrow(
                f"""
                SELECT id, label, enrichment_score FROM {table}
                WHERE external_ids->>'hubspot_id' = $1 AND id != $2
                """,
                str(external_ids["hubspot_id"]),
                entity_id,
            )
            if row:
                matches.append({"type": "hubspot_id", "id": row["id"], "label": row["label"], "score": row["enrichment_score"] or 0.5})

        if external_ids.get("kbo_id"):
            row = await conn.fetchrow(
                f"""
                SELECT id, label, enrichment_score FROM {table}
                WHERE external_ids->>'kbo_id' = $1 AND id != $2
                """,
                external_ids["kbo_id"],
                entity_id,
            )
            if row:
                matches.append({"type": "kbo_id", "id": row["id"], "label": row["label"], "score": row["enrichment_score"] or 0.5})

        if external_ids.get("vat_number"):
            row = await conn.fetchrow(
                f"""
                SELECT id, label, enrichment_score FROM {table}
                WHERE external_ids->>'vat_number' = $1 AND id != $2
                """,
                external_ids["vat_number"],
                entity_id,
            )
            if row:
                matches.append({"type": "vat_number", "id": row["id"], "label": row["label"], "score": row["enrichment_score"] or 0.5})

        if not matches and entity_attributes.get("label") and entity_attributes.get("website"):
            name = entity_attributes["label"].lower().strip()
            website = entity_attributes["website"].lower().strip()
            row = await conn.fetchrow(
                f"""
                SELECT id, label, enrichment_score,
                       similarity(lower(entity_attributes->>'label'), $1) as name_sim,
                       similarity(lower(entity_attributes->>'website'), $2) as website_sim
                FROM {table}
                WHERE id != $3
                ORDER BY name_sim DESC, website_sim DESC
                LIMIT 1
                """,
                name, website, entity_id
            )
            if row and row["name_sim"] and row["website_sim"]:
                combined = (float(row["name_sim"]) + float(row["website_sim"])) / 2
                if combined >= 0.5:
                    matches.append({
                        "type": "fuzzy",
                        "id": row["id"],
                        "label": row["label"],
                        "score": combined,
                        "confidence": "high" if combined >= 0.9 else ("medium" if combined >= 0.5 else "low")
                    })

        action: Literal["auto_merge", "flag_review", "none"] = "none"
        if matches:
            best = max(matches, key=lambda m: m["score"])
            if best["score"] >= 0.9:
                action = "auto_merge"
            elif best["score"] >= 0.5:
                action = "flag_review"

        if action != "none":
            await conn.execute(
                """
                INSERT INTO merge_history (source_entity_id, target_entity_id, match_type, confidence_score, status, created_at)
                VALUES ($1, $2, $3, $4, $5, NOW())
                """,
                entity_id, best["id"], best.get("type", "unknown"), best["score"],
                "auto_merged" if action == "auto_merge" else "pending_review"
            )

        return {"action": action, "best_match": matches[0] if matches else None, "all_matches": matches}
    finally:
        await conn.close()
