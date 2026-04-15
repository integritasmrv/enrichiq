#!/usr/bin/env python3
import os, sys
import psycopg2

CRM_DBS = {
    'MRV': {
        'host': '10.0.13.2', 'port': 5432,
        'dbname': 'integritasmrv_crm', 'user': 'integritasmrv_crm_user',
        'password': 'Int3gr1t@smrv_S3cure_P@ssw0rd_2026'
    },
    'Power': {
        'host': '10.0.14.2', 'port': 5432,
        'dbname': 'poweriq_crm', 'user': 'poweriq_crm_user',
        'password': 'P0w3r1Q_CRM_S3cur3_P@ss_2026'
    }
}

TRIGGER_FUNCTION = '''
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
'''

TRIGGER_SQL_CONTACTS = '''
DROP TRIGGER IF EXISTS trg_notify_enrichment_contact ON nb_crm_contacts;
CREATE TRIGGER trg_notify_enrichment_contact
  AFTER INSERT OR UPDATE ON nb_crm_contacts
  FOR EACH ROW
  WHEN (NEW.enrichment_status = 'pending')
  EXECUTE FUNCTION fn_notify_enrichment_queue();
'''

TRIGGER_SQL_CUSTOMERS = '''
DROP TRIGGER IF EXISTS trg_notify_enrichment_company ON nb_crm_customers;
CREATE TRIGGER trg_notify_enrichment_company
  AFTER INSERT OR UPDATE ON nb_crm_customers
  FOR EACH ROW
  WHEN (NEW.enrichment_status = 'pending')
  EXECUTE FUNCTION fn_notify_enrichment_queue();
'''

def create_triggers():
    for crm_name, db_config in CRM_DBS.items():
        print(f"\n=== Setting up triggers for {crm_name} ===")
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        
        try:
            cur.execute(TRIGGER_FUNCTION)
            print(f"Function created in {crm_name}")
        except Exception as e:
            print(f"Function error in {crm_name}: {e}")
        
        try:
            cur.execute(TRIGGER_SQL_CONTACTS)
            print(f"Contacts trigger created in {crm_name}")
        except Exception as e:
            print(f"Contacts trigger error in {crm_name}: {e}")
        
        try:
            cur.execute(TRIGGER_SQL_CUSTOMERS)
            print(f"Customers trigger created in {crm_name}")
        except Exception as e:
            print(f"Customers trigger error in {crm_name}: {e}")
        
        conn.commit()
        cur.close()
        conn.close()
        print(f"Completed {crm_name}")

if __name__ == '__main__':
    create_triggers()
