import asyncio
import asyncpg
import json
from datetime import datetime


CRM_DBS = [
    {
        "name": "integritasmrv",
        "host": "10.0.13.2",
        "port": 5432,
        "db": "integritasmrv_crm",
        "user": "integritasmrv_crm_user",
        "password": "Int3gr1t@smrv_S3cure_P@ssw0rd_2026",
    },
    {
        "name": "poweriq",
        "host": "10.0.14.2",
        "port": 5432,
        "db": "poweriq_crm",
        "user": "poweriq_crm_user",
        "password": "P0w3r1Q_CRM_S3cur3_P@ss_2026",
    },
]


async def listen_crm(db: dict, temporal_addr: str, task_queue: str):
    conn = await asyncpg.connect(
        host=db["host"],
        port=db["port"],
        database=db["db"],
        user=db["user"],
        password=db["password"],
    )
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )

    from temporalio.client import Client

    async with conn.listen("enrichment_queue") as channel:
        print(f"[{db['name']}] Listening on enrichment_queue...")
        async for notify in channel:
            try:
                payload_str = notify.payload.decode()
                parts = payload_str.split("|")
                if len(parts) < 2:
                    print(f"[{db['name']}] Invalid payload: {payload_str}")
                    continue

                entity_id = int(parts[0])
                hubspot_id = parts[1] if len(parts) > 1 else None
                timestamp = parts[2] if len(parts) > 2 else datetime.utcnow().isoformat()

                client = await Client.connect(temporal_addr)
                await client.start_workflow(
                    "EnrichmentWorkflow",
                    {
                        "entity_id": entity_id,
                        "crm_name": db["name"],
                        "hubspot_id": hubspot_id,
                        "timestamp": timestamp,
                    },
                    id=f"enrich-{db['name']}-{entity_id}-{timestamp.replace(':', '')}",
                    task_queue=task_queue,
                )
                print(f"[{db['name']}] Triggered EnrichmentWorkflow for entity {entity_id}")
            except Exception as e:
                print(f"[{db['name']}] Error processing notification: {e}")


async def main():
    task_queue = "integritasmrv-ingest"
    temporal_addr = "10.0.4.16:7233"
    await asyncio.gather(
        *[listen_crm(db, temporal_addr, task_queue) for db in CRM_DBS]
    )


if __name__ == "__main__":
    asyncio.run(main())
