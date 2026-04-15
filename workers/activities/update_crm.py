import asyncpg
from temporalio import activity


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


@activity.defn
async def update_crm_enrichment(
    entity_id: int,
    entity_attributes: dict,
    enrichment_status: str,
    enrichment_score: float = None,
    target_crm: str = "integritasmrv",
    table: str = "nb_crm_contacts",
) -> bool:
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
        query = f"""
            UPDATE {table} SET
                entity_attributes = $2,
                enrichment_status = $3,
                enrichment_score = COALESCE($4, enrichment_score),
                last_enriched_at = NOW(),
                updated_at = NOW()
            WHERE id = $1
        """
        result = await conn.execute(
            query, entity_id, entity_attributes, enrichment_status, enrichment_score
        )
        return result == "UPDATE 1"
    finally:
        await conn.close()
