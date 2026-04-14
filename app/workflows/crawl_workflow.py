from datetime import timedelta

from temporalio import workflow


@workflow.defn
class CrawlWorkflow:
    @workflow.run
    async def run(self, request_id: int) -> dict:
        await workflow.execute_activity(
            "init_crawl_session",
            args=[request_id],
            start_to_close_timeout=timedelta(seconds=20),
        )

        await workflow.execute_activity(
            "seed_frontier_from_searxng",
            args=[request_id],
            start_to_close_timeout=timedelta(minutes=5),
        )

        while True:
            should_stop = await workflow.execute_activity(
                "check_stop_conditions",
                args=[request_id],
                start_to_close_timeout=timedelta(seconds=15),
            )
            if should_stop:
                break

            url_item = await workflow.execute_activity(
                "pop_frontier_url",
                args=[request_id],
                start_to_close_timeout=timedelta(seconds=15),
            )
            if not url_item:
                break

            result = await workflow.execute_activity(
                "fetch_and_process_url",
                args=[request_id, url_item],
                start_to_close_timeout=timedelta(minutes=3),
            )

            links = result.get("links") if isinstance(result, dict) else []
            if links:
                await workflow.execute_activity(
                    "score_and_enqueue_links",
                    args=[request_id, links, int(url_item.get("crawl_depth", 0)) + 1],
                    start_to_close_timeout=timedelta(minutes=2),
                )

        final = await workflow.execute_activity(
            "finalize_crawl",
            args=[request_id],
            start_to_close_timeout=timedelta(seconds=40),
        )
        return final
