from temporalio import workflow


@workflow.defn
class IngestWorkflow:
    @workflow.run
    async def run(self, payload: dict) -> dict:
        from workers.activities.apply_mapping import apply_mapping
        from workers.activities.upsert_crm import upsert_crm_entity

        source = payload.get("source", "hubspot")
        mapping_name = payload.get("mapping_name", "hubspot_to_crm")
        target_crm = payload.get("target_crm", "integritasmrv")
        business_key = payload.get("business_key")

        mapped = apply_mapping(payload.get("data", payload), mapping_name)
        mapped["enrichment_status"] = "pending"

        result = await upsert_crm_entity(
            mapped_data=mapped,
            target_crm=target_crm,
            business_key_value=business_key,
        )

        return {
            "entity_id": result["id"],
            "created_at": str(result["created"]),
            "target_crm": target_crm,
            "enrichment_status": "pending",
        }
