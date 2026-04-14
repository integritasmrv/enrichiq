from temporalio import workflow
from datetime import timedelta


@workflow.defn
class SpiderFootWorkflow:
    @workflow.run
    async def run(self, request_id: int, target: str) -> dict:
        events = await workflow.execute_activity(
            "run_spiderfoot_scan",
            args=[request_id, target],
            start_to_close_timeout=timedelta(minutes=25),
        )
        stored = await workflow.execute_activity(
            "store_spiderfoot_results",
            args=[request_id, events],
            start_to_close_timeout=timedelta(minutes=5),
        )
        return {
            "request_id": request_id,
            "target": target,
            "events_stored": int(stored or 0),
            "status": "finished",
        }
