import asyncio
import asyncpg
import json
from datetime import datetime
from temporalio.client import Client


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

NOTIFICATION_QUEUE = "enrichment_queue"
TASK_QUEUE = "integritasmrv-ingest"
TEMPORAL_ADDR = "10.0.4.16:7233"


async def handle_notification(connection, channel, payload, worker_id):
    try:
        payload_str = payload.decode()
        parts = payload_str.split("|")
        if len(parts) < 2:
            print(f"[{channel}] Invalid payload: {payload_str}")
            return

        entity_id = int(parts[0])
        hubspot_id = parts[1] if len(parts) > 1 else None
        timestamp = parts[2] if len(parts) > 2 else datetime.utcnow().isoformat()

        client = await Client.connect(TEMPORAL_ADDR)
        await client.start_workflow(
            "EnrichmentWorkflow",
            {
                "entity_id": entity_id,
                "crm_name": channel,
                "hubspot_id": hubspot_id,
                "timestamp": timestamp,
            },
            id=f"enrich-{channel}-{entity_id}-{timestamp.replace(':', '').replace('.', '')}",
            task_queue=TASK_QUEUE,
        )
        print(f"[{channel}] Triggered EnrichmentWorkflow for entity {entity_id}")
    except Exception as e:
        print(f"[{channel}] Error: {e}")


async def listen_crm(db: dict):
    conn = await asyncpg.connect(
        host=db["host"],
        port=db["port"],
        database=db["db"],
        user=db["user"],
        password=db["password"],
    )
    
    def notification_handler(connection, channel, payload, worker_id):
        asyncio.create_task(handle_notification(connection, channel, payload, worker_id))
    
    await conn.add_listener(NOTIFICATION_QUEUE, notification_handler)
    print(f"[{db['name']}] Listening on {NOTIFICATION_QUEUE}...")
    
    try:
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        await conn.close()


async def main():
    await asyncio.gather(*[listen_crm(db) for db in CRM_DBS])


if __name__ == "__main__":
    asyncio.run(main())
