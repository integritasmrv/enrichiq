from datetime import timedelta
from temporalio import workflow


@workflow.defn
class WritebackWorkflow:
    @workflow.run
    async def run(self, payload: dict) -> dict:
        update_crm_mod = __import__('workers.activities.update_crm', fromlist=['update_crm_enrichment'])
        update_crm_enrichment = update_crm_mod.update_crm_enrichment
        dedup_merge_mod = __import__('workers.activities.dedup_merge_check', fromlist=['dedup_merge_check'])
        dedup_merge_check = dedup_merge_mod.dedup_merge_check
        hubspot_mod = __import__('workers.activities.update_hubspot', fromlist=['update_hubspot_contact', 'update_hubspot_company'])
        update_hubspot_contact = hubspot_mod.update_hubspot_contact
        update_hubspot_company = hubspot_mod.update_hubspot_company

        entity_id = payload["entity_id"]
        target_crm = payload.get("target_crm", "integritasmrv")
        source_system = payload.get("source_system", "hubspot")
        entity_type = payload.get("entity_type", "contact")
        enriched_data = payload.get("enriched_data", {})
        external_ids = payload.get("external_ids", {})

        ea = enriched_data.get("entity_attributes", {})
        ea["enrichment_score"] = enriched_data.get("enrichment_score")
        ea["last_enriched_at"] = enriched_data.get("last_enriched_at")

        timeout = timedelta(seconds=30)

        update_result = await workflow.execute_activity(
            update_crm_enrichment,
            args=[entity_id, ea, "enriched", enriched_data.get("enrichment_score"), target_crm],
            start_to_close_timeout=timeout,
        )

        dedup_result = await workflow.execute_activity(
            dedup_merge_check,
            args=[entity_id, external_ids, ea, target_crm],
            start_to_close_timeout=timeout,
        )

        hubspot_result = {"updated": False}
        if source_system == "hubspot":
            if entity_type == "contact" and external_ids.get("hubspot_id"):
                hubspot_result = await workflow.execute_activity(
                    update_hubspot_contact,
                    args=[external_ids["hubspot_id"], enriched_data],
                    start_to_close_timeout=timeout,
                )
            elif entity_type == "company" and external_ids.get("hubspot_company_id"):
                hubspot_result = await workflow.execute_activity(
                    update_hubspot_company,
                    args=[external_ids["hubspot_company_id"], enriched_data],
                    start_to_close_timeout=timeout,
                )

        return {
            "crm_updated": update_result,
            "dedup": dedup_result,
            "hubspot_synced": hubspot_result,
        }
