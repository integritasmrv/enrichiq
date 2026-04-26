#!/usr/bin/env python3
"""
Production Pipeline for Company Register Data
=============================================
Simple, fast COPY-based merge.
Strategy: COPY to staging -> DELETE matching -> INSERT FROM staging
"""
import sys
import os
import logging
from datetime import datetime
import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
DB_PORT = int(os.getenv('DB_PORT', '5434'))
DB_USER = os.getenv('DB_USER', 'aiuser')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'aipassword123')
MASTER_DB = 'BE KBO MASTER'
METRICS_DB = 'RUNS-METRICS'

TABLES = [
    ("Enterprise.csv",  "kbo_master.enterprise",     "kbo.enterprise"),
    ("Establishment.csv","kbo_master.establishment",  "kbo.establishment"),
    ("Denomination.csv", "kbo_master.denomination",   "kbo.denomination"),
    ("Address.csv",      "kbo_master.address",        "kbo.address"),
    ("Contact.csv",      "kbo_master.contact",        "kbo.contact"),
    ("Activity.csv",     "kbo_master.activity",       "kbo.activity"),
    ("Branch.csv",       "kbo_master.branch",         "kbo.branch"),
    ("Code.csv",         "kbo_master.code",           "kbo.code"),
]

VERSION_COLS = {
    'kbo.branch': 'EnterpriseNumber,EstablishmentNumber,StartDate',
}

MASTER_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS kbo_master.enterprise (
        enterprisenumber VARCHAR(100) PRIMARY KEY, status VARCHAR(2),
        juridicalsituation VARCHAR(10), typeofenterprise VARCHAR(3),
        juridicalform VARCHAR(10), juridicalformcac VARCHAR(10), startdate VARCHAR(20))""",
    """CREATE TABLE IF NOT EXISTS kbo_master.establishment (
        establishmentnumber VARCHAR(100) PRIMARY KEY, startdate VARCHAR(20),
        enterprisenumber VARCHAR(100))""",
    """CREATE TABLE IF NOT EXISTS kbo_master.denomination (
        entitynumber VARCHAR(100), language VARCHAR(3),
        typeofdenomination VARCHAR(10), denomination VARCHAR(500))""",
    """CREATE TABLE IF NOT EXISTS kbo_master.address (
        entitynumber VARCHAR(100), typeofaddress VARCHAR(20),
        countrynl VARCHAR(100), countryfr VARCHAR(100), zipcode VARCHAR(20),
        municipalitynl VARCHAR(100), municipalityfr VARCHAR(100),
        streetnl VARCHAR(500), streetfr VARCHAR(500),
        housenumber VARCHAR(20), box VARCHAR(20),
        extraaddressinfo VARCHAR(500), datestrikingoff VARCHAR(20))""",
    """CREATE TABLE IF NOT EXISTS kbo_master.contact (
        entitynumber VARCHAR(100), entitycontact VARCHAR(20),
        contacttype VARCHAR(20), value VARCHAR(500))""",
    """CREATE TABLE IF NOT EXISTS kbo_master.activity (
        entitynumber VARCHAR(100), activitygroup VARCHAR(20),
        naceversion VARCHAR(10), nacecode VARCHAR(20),
        classification VARCHAR(20))""",
    """CREATE TABLE IF NOT EXISTS kbo_master.branch (
        id SERIAL PRIMARY KEY, enterprisenumber VARCHAR(100),
        establishmentnumber VARCHAR(100), startdate VARCHAR(20))""",
    """CREATE TABLE IF NOT EXISTS kbo_master.code (
        category VARCHAR(50), code VARCHAR(50),
        language VARCHAR(3), description VARCHAR(500))""",
]

PK_COLS = {
    'enterprise': ['enterprisenumber'],
    'establishment': ['establishmentnumber'],
    'denomination': ['entitynumber', 'language', 'typeofdenomination'],
    'address': ['entitynumber', 'typeofaddress'],
    'contact': ['entitynumber', 'entitycontact', 'contacttype'],
    'activity': ['entitynumber', 'activitygroup', 'naceversion', 'nacecode'],
    'branch': ['establishmentnumber'],
    'code': ['category', 'code', 'language'],
}

# Mapping from version column name (CamelCase) -> master column name (lowercase)
COL_MAP = {
    'EnterpriseNumber': 'enterprisenumber', 'Status': 'status',
    'JuridicalSituation': 'juridicalsituation', 'TypeOfEnterprise': 'typeofenterprise',
    'JuridicalForm': 'juridicalform', 'JuridicalFormCAC': 'juridicalformcac',
    'StartDate': 'startdate', 'EstablishmentNumber': 'establishmentnumber',
    'Language': 'language', 'TypeOfDenomination': 'typeofdenomination',
    'Denomination': 'denomination', 'EntityNumber': 'entitynumber',
    'TypeOfAddress': 'typeofaddress', 'CountryNL': 'countrynl',
    'CountryFR': 'countryfr', 'Zipcode': 'zipcode',
    'MunicipalityNL': 'municipalitynl', 'MunicipalityFR': 'municipalityfr',
    'StreetNL': 'streetnl', 'StreetFR': 'streetfr',
    'HouseNumber': 'housenumber', 'Box': 'box',
    'ExtraAddressInfo': 'extraaddressinfo', 'DateStrikingOff': 'datestrikingoff',
    'EntityContact': 'entitycontact', 'ContactType': 'contacttype',
    'Value': 'value', 'ActivityGroup': 'activitygroup',
    'NaceVersion': 'naceversion', 'NaceCode': 'nacecode',
    'Classification': 'classification', 'Category': 'category',
    'Code': 'code', 'Description': 'description',
}


def get_conn(db):
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, database=db, user=DB_USER, password=DB_PASSWORD)


def init_master():
    logger.info("Initializing master database...")
    conn = get_conn("postgres")
    conn.set_isolation_level(0)
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE DATABASE "{MASTER_DB}"')
            cur.execute(f'CREATE DATABASE "{METRICS_DB}"')
    except psycopg2.errors.DuplicateDatabase:
        pass
    conn.close()

    conn = get_conn(MASTER_DB)
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS kbo_master")
        for sql in MASTER_SCHEMA:
            cur.execute(sql)
    conn.commit()
    conn.close()

    metrics_conn = get_conn(METRICS_DB)
    with metrics_conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS pipeline_state (
            id SERIAL PRIMARY KEY, extract_version VARCHAR(50) UNIQUE NOT NULL,
            status VARCHAR(20) DEFAULT 'pending', load_started_at TIMESTAMP,
            load_completed_at TIMESTAMP, merge_started_at TIMESTAMP,
            merge_completed_at TIMESTAMP, records_loaded BIGINT, records_merged BIGINT,
            error_message TEXT, created_at TIMESTAMP DEFAULT NOW())""")
        cur.execute("""CREATE TABLE IF NOT EXISTS pipeline_metrics (
            id SERIAL PRIMARY KEY, extract_version VARCHAR(50) NOT NULL,
            table_name VARCHAR(50) NOT NULL, operation VARCHAR(20) NOT NULL,
            rows_count BIGINT NOT NULL, rows_inserted BIGINT DEFAULT 0,
            rows_updated BIGINT DEFAULT 0, status VARCHAR(20) DEFAULT 'completed',
            recorded_at TIMESTAMP DEFAULT NOW())""")
        cur.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            pipeline_name VARCHAR(100) NOT NULL, pipeline_version VARCHAR(20) NOT NULL,
            source_type VARCHAR(50) NOT NULL, run_type VARCHAR(30) NOT NULL DEFAULT 'initial',
            status VARCHAR(20) NOT NULL DEFAULT 'planned', progress_percent DECIMAL(5,2) DEFAULT 0,
            total_items BIGINT DEFAULT 0, processed_items BIGINT DEFAULT 0,
            total_batches INT DEFAULT 0, processed_batches INT DEFAULT 0,
            total_files INT DEFAULT 0, processed_files INT DEFAULT 0,
            server_load_percent DECIMAL(5,2), batch_size INT DEFAULT 50,
            source_config JSONB DEFAULT '{}', started_at TIMESTAMP, finished_at TIMESTAMP,
            error_message TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS pipeline_run_items (
            item_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            run_id UUID REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
            item_type VARCHAR(30) NOT NULL DEFAULT 'file', source_path VARCHAR(500),
            table_name VARCHAR(100), status VARCHAR(20) NOT NULL DEFAULT 'pending',
            progress_percent DECIMAL(5,2) DEFAULT 0, total_items BIGINT DEFAULT 0,
            processed_items BIGINT DEFAULT 0, pagination_state JSONB DEFAULT '{}',
            started_at TIMESTAMP, finished_at TIMESTAMP, error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
    metrics_conn.commit()
    metrics_conn.close()
    logger.info("Master and metrics databases initialized.")


def get_version_db(label):
    return f"BE KBO {label}"


def create_version_db(label):
    dbname = get_version_db(label)
    logger.info(f"Creating versioned database: {dbname}")
    conn = get_conn("postgres")
    conn.set_isolation_level(0)
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
            cur.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        conn.close()

    conn = get_conn(dbname)
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS kbo")
        for sql in [
            """CREATE TABLE kbo.enterprise (
                EnterpriseNumber VARCHAR(100) PRIMARY KEY, Status VARCHAR(2),
                JuridicalSituation VARCHAR(10), TypeOfEnterprise VARCHAR(3),
                JuridicalForm VARCHAR(10), JuridicalFormCAC VARCHAR(10), StartDate VARCHAR(20))""",
            """CREATE TABLE kbo.establishment (
                EstablishmentNumber VARCHAR(100) PRIMARY KEY, StartDate VARCHAR(20),
                EnterpriseNumber VARCHAR(100))""",
            """CREATE TABLE kbo.denomination (
                EntityNumber VARCHAR(100), Language VARCHAR(3),
                TypeOfDenomination VARCHAR(10), Denomination VARCHAR(500))""",
            """CREATE TABLE kbo.address (
                EntityNumber VARCHAR(100), TypeOfAddress VARCHAR(20),
                CountryNL VARCHAR(100), CountryFR VARCHAR(100), Zipcode VARCHAR(20),
                MunicipalityNL VARCHAR(100), MunicipalityFR VARCHAR(100),
                StreetNL VARCHAR(500), StreetFR VARCHAR(500),
                HouseNumber VARCHAR(20), Box VARCHAR(20),
                ExtraAddressInfo VARCHAR(500), DateStrikingOff VARCHAR(20))""",
            """CREATE TABLE kbo.contact (
                EntityNumber VARCHAR(100), EntityContact VARCHAR(20),
                ContactType VARCHAR(20), Value VARCHAR(500))""",
            """CREATE TABLE kbo.activity (
                EntityNumber VARCHAR(100), ActivityGroup VARCHAR(20),
                NaceVersion VARCHAR(10), NaceCode VARCHAR(20),
                Classification VARCHAR(20))""",
            """CREATE TABLE kbo.branch (
                Id SERIAL PRIMARY KEY, EnterpriseNumber VARCHAR(100),
                EstablishmentNumber VARCHAR(100), StartDate VARCHAR(20))""",
            """CREATE TABLE kbo.code (
                Category VARCHAR(50), Code VARCHAR(50),
                Language VARCHAR(3), Description VARCHAR(500))""",
        ]:
            cur.execute(sql)
    conn.commit()
    conn.close()
    return dbname


def load_csv(conn, csv_path, table, cols=None):
    col_clause = f"({cols})" if cols else ""
    with open(csv_path, 'r', encoding='latin-1', errors='replace') as f:
        with conn.cursor() as cur:
            cur.copy_expert(f"COPY {table} {col_clause} FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',', QUOTE '\"', NULL '')", f)
    conn.commit()


def load_extract(extract_path, label):
    dbname = get_version_db(label)
    state_conn = get_conn(METRICS_DB)

    with state_conn.cursor() as cur:
        cur.execute("SELECT status FROM public.pipeline_state WHERE extract_version = %s", (label,))
        row = cur.fetchone()
        if row and row[0] in ('loaded', 'merging', 'merged'):
            logger.info(f"Version {label} already {row[0]}, skipping load")
            return False

    with state_conn.cursor() as cur:
        cur.execute("""INSERT INTO public.pipeline_state (extract_version, status, load_started_at)
            VALUES (%s, 'loading', NOW())
            ON CONFLICT (extract_version) DO UPDATE SET status = 'loading', load_started_at = NOW()""", (label,))
        state_conn.commit()

    try:
        create_version_db(label)
        total = 0
        for csv_file, master_table, version_table in TABLES:
            csv_path = os.path.join(extract_path, csv_file)
            table_name = version_table.split('.')[1]
            if not os.path.exists(csv_path):
                logger.warning(f"CSV not found: {csv_path}, skipping")
                with state_conn.cursor() as cur:
                    cur.execute("""INSERT INTO public.pipeline_metrics 
                        (extract_version, table_name, operation, rows_count, rows_inserted, rows_updated, status)
                        VALUES (%s, %s, 'LOAD', 0, 0, 0, 'skipped')""", (label, table_name))
                state_conn.commit()
                continue
            try:
                logger.info(f"Loading {csv_file}...")
                conn = get_conn(dbname)
                cols = VERSION_COLS.get(version_table)
                load_csv(conn, csv_path, version_table, cols)
                with conn.cursor() as cur:
                    cur.execute(f"SELECT COUNT(*) FROM {version_table}")
                    cnt = cur.fetchone()[0]
                conn.close()
                logger.info(f"  -> {cnt:,} rows")
                total += cnt
                with state_conn.cursor() as cur:
                    cur.execute("""INSERT INTO public.pipeline_metrics 
                        (extract_version, table_name, operation, rows_count, rows_inserted, rows_updated, status)
                        VALUES (%s, %s, 'LOAD', %s, %s, 0, 'completed')""", 
                        (label, table_name, cnt, cnt))
                state_conn.commit()
            except Exception as e:
                logger.error(f"  Failed to load {csv_file}: {e}")
                with state_conn.cursor() as cur:
                    cur.execute("""INSERT INTO public.pipeline_metrics 
                        (extract_version, table_name, operation, rows_count, rows_inserted, rows_updated, status)
                        VALUES (%s, %s, 'LOAD', 0, 0, 0, 'failed')""", (label, table_name))
                state_conn.commit()

        with state_conn.cursor() as cur:
            cur.execute("UPDATE public.pipeline_state SET status = 'loaded', load_completed_at = NOW(), records_loaded = %s WHERE extract_version = %s", (total, label))
            state_conn.commit()
        state_conn.close()
        logger.info(f"Load complete: {total:,} total records")
        return True
    except Exception as e:
        with state_conn.cursor() as cur:
            cur.execute("UPDATE public.pipeline_state SET status = 'failed', error_message = %s WHERE extract_version = %s", (str(e), label))
            state_conn.commit()
        state_conn.close()
        raise


def merge_extract(label):
    """Fast merge: COPY source -> staging -> DELETE+INSERT"""
    dbname = get_version_db(label)
    master_conn = get_conn(MASTER_DB)
    version_conn = get_conn(dbname)
    metrics_conn = get_conn(METRICS_DB)

    with metrics_conn.cursor() as cur:
        cur.execute("SELECT status FROM public.pipeline_state WHERE extract_version = %s", (label,))
        row = cur.fetchone()
        if row and row[0] == 'merged':
            logger.info(f"Version {label} already merged, skipping")
            return False

    with metrics_conn.cursor() as cur:
        cur.execute("UPDATE public.pipeline_state SET status = 'merging', merge_started_at = NOW() WHERE extract_version = %s", (label,))
        metrics_conn.commit()

    # Get list of already-merged tables to skip
    with metrics_conn.cursor() as cur:
        cur.execute("SELECT table_name FROM public.pipeline_metrics WHERE extract_version = %s AND operation = 'MERGED' AND status = 'completed'", (label,))
        merged_tables = set(r[0] for r in cur.fetchall())

    total_ops = 0
    try:
        for csv_file, master_table, version_table in TABLES:
            table_name = master_table.split('.')[1]
            if table_name in merged_tables:
                logger.info(f"Skipping {master_table} (already merged)")

                continue
            pkeys = PK_COLS.get(table_name, [])
            
            logger.info(f"Merging {master_table}...")

            # Get source columns
            with version_conn.cursor() as cur:
                schema, tbl = version_table.split('.')
                cur.execute("""SELECT column_name FROM information_schema.columns 
                    WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position""", (schema, tbl))
                src_cols = [r[0] for r in cur.fetchall()]

            # Get source count
            with version_conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {version_table}")
                source_count = cur.fetchone()[0]
            
            # Get master count before
            with master_conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {master_table}")
                master_before = cur.fetchone()[0]

            logger.info(f"  Source: {source_count:,}, Master before: {master_before:,}")

            # Export to CSV
            temp_file = f'/tmp/{table_name}_merge.csv'
            with version_conn.cursor() as cur:
                cur.copy_expert(f"COPY (SELECT {', '.join(src_cols)} FROM {version_table}) TO STDOUT WITH (FORMAT CSV, HEADER, DELIMITER ',')", open(temp_file, 'w', encoding='utf-8'))

            logger.info(f"  Exported to {temp_file}")

            # Create temp table in master
            staging_table = f'kbo_merge_staging_{table_name}'
            with master_conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {staging_table}")
                col_defs = [f'col{i} TEXT' for i in range(len(src_cols))]
                cur.execute(f"CREATE TABLE {staging_table} ({', '.join(col_defs)})")
            master_conn.commit()

            # COPY into staging
            with open(temp_file, 'r', encoding='utf-8') as f:
                with master_conn.cursor() as cur:
                    cur.copy_expert(f"COPY {staging_table} FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',', NULL '')", f)
            master_conn.commit()
            os.remove(temp_file)

            # Get staging count
            with master_conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {staging_table}")
                staging_count = cur.fetchone()[0]
            logger.info(f"  Staged: {staging_count:,}")

            # Build DELETE + INSERT
            if pkeys:
                # Map version column name (CamelCase) -> master column position
                pk_master_cols = [COL_MAP.get(pk, pk).lower() for pk in pkeys if pk in src_cols or COL_MAP.get(pk) is not None]
                # Find position of each PK column in src_cols for staging reference
                pk_idx_in_staging = [src_cols.index(pk) if pk in src_cols else src_cols.index(next(k for k, v in COL_MAP.items() if v == pk.lower()))
                    for pk in pkeys]
                where_parts = [f"m.{pk_master_cols[i]} = s.col{pk_idx_in_staging[i]}" for i in range(len(pkeys))]
                where_clause = ' AND '.join(where_parts)
                
                # Delete matching from master
                with master_conn.cursor() as cur:
                    cur.execute(f"""
                        DELETE FROM {master_table} m
                        WHERE EXISTS (SELECT 1 FROM {staging_table} s WHERE {where_clause})
                    """)
                    deleted = cur.rowcount
                master_conn.commit()
                logger.info(f"  Deleted: {deleted:,}")
            else:
                deleted = 0

            # Insert all from staging
            col_list = ', '.join([f'col{i}' for i in range(len(src_cols))])
            with master_conn.cursor() as cur:
                cur.execute(f"INSERT INTO {master_table} SELECT {col_list} FROM {staging_table}")
                inserted = cur.rowcount
            master_conn.commit()
            logger.info(f"  Inserted: {inserted:,}")

            # Cleanup
            with master_conn.cursor() as cur:
                cur.execute(f"DROP TABLE {staging_table}")
            master_conn.commit()

            # Get final count
            with master_conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {master_table}")
                master_after = cur.fetchone()[0]

            # Record metrics with rows_inserted vs rows_updated
            rows_inserted = inserted - deleted if inserted > deleted else 0
            rows_updated = deleted
            with metrics_conn.cursor() as cur:
                cur.execute("""INSERT INTO public.pipeline_metrics 
                    (extract_version, table_name, operation, rows_count, rows_inserted, rows_updated, status) 
                    VALUES (%s, %s, 'MERGED', %s, %s, %s, 'completed')""", 
                    (label, table_name, source_count, rows_inserted, rows_updated))
            metrics_conn.commit()

            logger.info(f"  {master_table}: {source_count:,} merged ({deleted:,} replaced, {inserted:,} new net)")
            total_ops += source_count

        with metrics_conn.cursor() as cur:
            cur.execute("""UPDATE public.pipeline_state 
                SET status = 'merged', merge_completed_at = NOW(), records_merged = %s 
                WHERE extract_version = %s""", (total_ops, label))
            metrics_conn.commit()

        logger.info(f"Merge complete: {total_ops:,} total")
        master_conn.close()
        version_conn.close()
        metrics_conn.close()
        return True

    except Exception as e:
        logger.error(f"Merge failed: {e}")
        import traceback
        traceback.print_exc()
        with master_conn.cursor() as cur:
            cur.execute("ROLLBACK")
        with metrics_conn.cursor() as cur:
            cur.execute("UPDATE public.pipeline_state SET status = 'failed', error_message = %s WHERE extract_version = %s", (str(e)[:500], label))
            metrics_conn.commit()
        master_conn.close()
        version_conn.close()
        metrics_conn.close()
        raise


def show_status():
    metrics_conn = get_conn(METRICS_DB)
    print("\n" + "="*70)
    print("PIPELINE STATUS")
    print("="*70)

    with metrics_conn.cursor() as cur:
        cur.execute("""SELECT extract_version, status, records_loaded, records_merged, error_message 
            FROM public.pipeline_state ORDER BY extract_version""")
        for row in cur.fetchall():
            print(f"\nVersion {row[0]}: {row[1]}")
            print(f"  Loaded: {row[2] or 0:,} records")
            print(f"  Merged: {row[3] or 0:,} records")
            if row[4]:
                print(f"  Error: {row[4]}")

    print("\n" + "-"*70)
    print("METRICS")
    print("-"*70)
    with metrics_conn.cursor() as cur:
        cur.execute("""SELECT extract_version, table_name, operation, SUM(rows_count) 
            FROM public.pipeline_metrics 
            GROUP BY extract_version, table_name, operation 
            ORDER BY extract_version, table_name""")
        for row in cur.fetchall():
            print(f"  {row[0]}/{row[1]}/{row[2]}: {row[3]:,}")
    metrics_conn.close()

    master_conn = get_conn(MASTER_DB)
    print("\n" + "-"*70)
    print("MASTER TABLE COUNTS")
    print("-"*70)
    with master_conn.cursor() as cur:
        cur.execute("""SELECT 'enterprise' as tbl, COUNT(*) FROM kbo_master.enterprise
            UNION ALL SELECT 'establishment', COUNT(*) FROM kbo_master.establishment
            UNION ALL SELECT 'address', COUNT(*) FROM kbo_master.address
            UNION ALL SELECT 'contact', COUNT(*) FROM kbo_master.contact
            UNION ALL SELECT 'activity', COUNT(*) FROM kbo_master.activity
            ORDER BY tbl""")
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]:,}")
    master_conn.close()
    print()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1]

    if command == "status":
        show_status()
    else:
        extract_path = command
        label = sys.argv[2]
        load_only = '--load-only' in sys.argv
        merge_only = '--merge-only' in sys.argv

        logger.info(f"Pipeline for version {label}")
        logger.info(f"Extract path: {extract_path}")

        init_master()

        if not merge_only:
            load_extract(extract_path, label)

        if not load_only:
            merge_extract(label)

        show_status()
