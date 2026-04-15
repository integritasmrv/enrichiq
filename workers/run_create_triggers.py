import asyncio
import asyncpg


CRM_DBS = [
    {
        "name": "MRV",
        "host": "10.0.13.2",
        "port": 5432,
        "db": "integritasmrv_crm",
        "user": "integritasmrv_crm_user",
        "password": "Int3gr1t@smrv_S3cure_P@ssw0rd_2026",
    },
    {
        "name": "Power",
        "host": "10.0.14.2",
        "port": 5432,
        "db": "poweriq_crm",
        "user": "poweriq_crm_user",
        "password": "P0w3r1Q_CRM_S3cur3_P@ss_2026",
    },
]


async def create_triggers_for_db(db: dict):
    conn = await asyncpg.connect(
        host=db["host"], port=db["port"], database=db["db"],
        user=db["user"], password=db["password"]
    )
    try:
        await conn.execute('''
            CREATE OR REPLACE FUNCTION fn_notify_enrichment_queue() RETURNS TRIGGER AS $$
            BEGIN
              IF NEW.enrichment_status = 'pending' THEN
                PERFORM pg_notify(
                  'enrichment_queue',
                  NEW.id || '|' || COALESCE((NEW.external_ids->>'hubspot_id'), '') || '|' || CURRENT_TIMESTAMP
                );
              END IF;
              RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
        ''')
        print(f"[{db['name']}] Function created")
    except Exception as e:
        print(f"[{db['name']}] Function error: {e}")

    for table in ["nb_crm_contacts", "nb_crm_customers"]:
        trigger_name = f"trg_notify_enrichment_{'contact' if 'contact' in table else 'company'}"
        try:
            await conn.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON {table};")
            await conn.execute(f'''
                CREATE TRIGGER {trigger_name}
                  AFTER INSERT OR UPDATE ON {table}
                  FOR EACH ROW WHEN (NEW.enrichment_status = 'pending')
                  EXECUTE FUNCTION fn_notify_enrichment_queue();
            ''')
            print(f"[{db['name']}] {table} trigger created")
        except Exception as e:
            print(f"[{db['name']}] {table} trigger error: {e}")

    await conn.close()


async def main():
    await asyncio.gather(*[create_triggers_for_db(db) for db in CRM_DBS])
    print("Done")


if __name__ == "__main__":
    asyncio.run(main())
