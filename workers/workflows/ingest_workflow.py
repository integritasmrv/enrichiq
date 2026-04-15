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

        if isinstance(mapped, dict) and "targets" in mapped.get("contact", {}):
            mapped = mapped["contact"]

        if isinstance(mapped, dict) and any(k in mapped for k in ("lead", "contact", "company")):
            results = {}
            for target_name, target_data in mapped.items():
                if not isinstance(target_data, dict):
                    continue
                table = target_data.pop("table", None)
                if table:
                    target_data["enrichment_status"] = "pending"
                    key_prefix = "webform" if source == "webform" else "hubspot"
                    key_value = f"{key_prefix}-{target_name}-{business_key}" if business_key else None
                    result = await upsert_crm_entity(
                        mapped_data=target_data,
                        target_crm=target_crm,
                        table=table,
                        business_key_value=key_value,
                    )
                    results[target_name] = result

            return {
                "lead_id": results.get("lead", {}).get("id"),
                "contact_id": results.get("contact", {}).get("id"),
                "company_id": results.get("company", {}).get("id"),
                "target_crm": target_crm,
                "source": source,
                "enrichment_status": "pending",
            }

        else:
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
