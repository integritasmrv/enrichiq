import asyncio
import os

from temporalio.client import Client
from temporalio.worker import Worker

from app.activities.crawl import (
    check_stop_conditions,
    fetch_and_process_url,
    finalize_crawl,
    init_crawl_session,
    pop_frontier_url,
    score_and_enqueue_links,
    seed_frontier_from_searxng,
)
from app.activities.spiderfoot import run_spiderfoot_scan, store_spiderfoot_results
from app.worker_runtime import (
    build_context_bundle,
    evaluate_confidence_gap,
    extract_fields_llm,
    get_eligible_collectors,
    promote_trusted_fields,
    run_collector,
    set_entity_status,
    write_hubspot,
)
from app.workflows import CrawlWorkflow, EnrichmentWorkflow, SpiderFootWorkflow

TEMPORAL_ADDRESS = os.environ.get(
    "TEMPORAL_HOST",
    os.environ.get("TEMPORAL_ADDRESS", "temporal-oo48k0844ok8k4cwc8o8wgk8:7233"),
)
TEMPORAL_NAMESPACE = os.environ.get("TEMPORAL_NAMESPACE", "default")

BUSINESS_KEY = os.environ.get("BUSINESS_KEY", "integritasmrv")
ENRICHMENT_QUEUE = os.environ.get("TASK_QUEUE_ENRICHMENT", f"enrichment-{BUSINESS_KEY}")
CRAWL_QUEUE = os.environ.get("TASK_QUEUE_CRAWL", "crawl")
SPIDERFOOT_QUEUE = os.environ.get("TASK_QUEUE_SPIDERFOOT", "spiderfoot")


async def main() -> None:
    client = await Client.connect(TEMPORAL_ADDRESS, namespace=TEMPORAL_NAMESPACE)

    worker_enrichment = Worker(
        client,
        task_queue=ENRICHMENT_QUEUE,
        workflows=[EnrichmentWorkflow],
        activities=[
            set_entity_status,
            build_context_bundle,
            get_eligible_collectors,
            run_collector,
            extract_fields_llm,
            promote_trusted_fields,
            evaluate_confidence_gap,
            write_hubspot,
        ],
    )

    worker_crawl = Worker(
        client,
        task_queue=CRAWL_QUEUE,
        workflows=[CrawlWorkflow],
        activities=[
            init_crawl_session,
            seed_frontier_from_searxng,
            check_stop_conditions,
            pop_frontier_url,
            fetch_and_process_url,
            score_and_enqueue_links,
            finalize_crawl,
        ],
    )

    worker_spiderfoot = Worker(
        client,
        task_queue=SPIDERFOOT_QUEUE,
        workflows=[SpiderFootWorkflow],
        activities=[run_spiderfoot_scan, store_spiderfoot_results],
    )

    await asyncio.gather(
        worker_enrichment.run(),
        worker_crawl.run(),
        worker_spiderfoot.run(),
    )

if __name__ == "__main__":
    asyncio.run(main())
