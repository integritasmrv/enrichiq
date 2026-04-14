from datetime import timedelta

from temporalio import workflow


@workflow.defn
class EnrichmentWorkflow:
    @workflow.run
    async def run(
        self, entity_id: str, source_system: str, round_number: int = 1, business_key: str = "integritasmrv"
    ) -> dict:
        await workflow.execute_activity(
            "set_entity_status",
            args=[entity_id, "in_progress"],
            start_to_close_timeout=timedelta(seconds=30),
        )

        current_round = max(1, int(round_number))
        rounds: list[dict] = []
        gap_status: dict = {}

        while current_round <= 3:
            context = await workflow.execute_activity(
                "build_context_bundle",
                args=[entity_id, current_round, business_key],
                start_to_close_timeout=timedelta(seconds=60),
            )

            collectors = await workflow.execute_activity(
                "get_eligible_collectors",
                args=[entity_id],
                start_to_close_timeout=timedelta(seconds=30),
            )

            for collector_name in collectors:
                await workflow.execute_activity(
                    "run_collector",
                    args=[collector_name, entity_id, context],
                    start_to_close_timeout=timedelta(minutes=20),
                )

            inserted = await workflow.execute_activity(
                "extract_fields_llm",
                args=[entity_id, current_round, business_key],
                start_to_close_timeout=timedelta(minutes=10),
            )

            trusted = await workflow.execute_activity(
                "promote_trusted_fields",
                args=[entity_id],
                start_to_close_timeout=timedelta(minutes=2),
            )

            gap_status = await workflow.execute_activity(
                "evaluate_confidence_gap",
                args=[entity_id, current_round],
                start_to_close_timeout=timedelta(seconds=30),
            )

            rounds.append(
                {
                    "round": current_round,
                    "collectors": collectors,
                    "inserted_attributes": inserted,
                    "trusted_promotions": trusted,
                    "gap": gap_status,
                }
            )

            if not bool(gap_status.get("should_continue")):
                break
            current_round += 1

        hubspot_sync = await workflow.execute_activity(
            "write_hubspot",
            args=[entity_id, source_system],
            start_to_close_timeout=timedelta(minutes=2),
        )

        await workflow.execute_activity(
            "set_entity_status",
            args=[entity_id, "done"],
            start_to_close_timeout=timedelta(seconds=30),
        )

        return {
            "entity_id": entity_id,
            "source_system": source_system,
            "business_key": business_key,
            "start_round": round_number,
            "rounds": rounds,
            "final_gap": gap_status,
            "hubspot_sync": hubspot_sync,
        }
