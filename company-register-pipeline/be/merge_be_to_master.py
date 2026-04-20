#!/usr/bin/env python3
"""
BE KBO Incremental Merge to Master

Always incremental - checks if record exists and compares for changes:
- First extract: insert only new records, update only changed records  
- Subsequent extracts: same logic, much less work

Row-by-row processing with batch commits to reduce database load.
"""

import os, sys, logging, argparse
from datetime import datetime
from typing import Dict, List, Tuple, Optional

import psycopg2

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from common import config_be as config
from common.db_utils import DBConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class KBOMergeToMaster:
    def __init__(self, db_config: DBConfig = None):
        self.db_config = db_config or DBConfig(
            host='127.0.0.1', port=5434, database='postgres',
            user='aiuser', password='aipassword123'
        )
        self.master_db = None
        self.versioned_db = None
        
    def connect(self):
        self.master_db = psycopg2.connect(
            host=self.db_config.host, port=self.db_config.port,
            database='BE KBO MASTER', user=self.db_config.user,
            password=self.db_config.password
        )
        self.master_db.autocommit = False
        
    def disconnect(self):
        if self.master_db: self.master_db.close()
        if self.versioned_db: self.versioned_db.close()

    def get_versioned_connection(self, versioned_db_name: str):
        return psycopg2.connect(
            host=self.db_config.host, port=self.db_config.port,
            database=versioned_db_name, user=self.db_config.user,
            password=self.db_config.password
        )

    def merge(self, version_id: str, snapshot_date: str,
              versioned_db_name: str, source_folder: str) -> Dict:
        logger.info(f"Starting incremental merge for extract {version_id}")
        
        self.connect()
        self.versioned_db = self.get_versioned_connection(versioned_db_name)
        
        try:
            total_inserted = total_updated = 0
            
            tables_config = [
                ('enterprise', 'EnterpriseNumber'),
                ('establishment', 'EstablishmentNumber'),
                ('denomination', 'EntityNumber'),
                ('address', 'EntityNumber'),
                ('contact', 'EntityNumber'),
                ('activity', 'EntityNumber'),
                ('branch', 'Id'),
                ('code', 'EntityNumber'),
            ]
            
            for table_name, pk_col in tables_config:
                logger.info(f"  Processing table: {table_name}")
                ins, upd = self._merge_table(
                    src_schema='kbo', dst_schema='kbo_master',
                    table_name=table_name, pk_col=pk_col, version_id=version_id
                )
                total_inserted += ins
                total_updated += upd
                logger.info(f"    {table_name}: {ins:,} inserted, {upd:,} updated")
            
            records_queued = self._sync_queue()
            
            self._update_extract_version(
                version_id, snapshot_date, source_folder,
                total_inserted, total_updated, records_queued
            )
            
            self.master_db.commit()
            
            return {
                'records_inserted': total_inserted,
                'records_updated': total_updated,
                'records_queued': records_queued
            }
            
        except Exception as e:
            logger.error(f"Merge failed: {e}")
            self.master_db.rollback()
            raise
        finally:
            self.disconnect()

    def _merge_table(self, src_schema: str, dst_schema: str,
                     table_name: str, pk_col: str, version_id: str,
                     batch_size: int = 500) -> Tuple[int, int]:
        cur_src = self.versioned_db.cursor()
        cur_dst = self.master_db.cursor()
        
        src_table = f"{src_schema}.{table_name}"
        dst_table = f"{dst_schema}.{table_name}"
        
        cur_src.execute(f"SELECT * FROM {src_table} LIMIT 1")
        cols = [desc[0] for desc in cur_src.description]
        cols_lower = [c.lower() for c in cols]
        
        col_list = ', '.join(cols_lower)
        placeholders = ', '.join(['%s'] * len(cols_lower))
        pk_lower = pk_col.lower()
        
        update_cols = [c for c in cols_lower if c != pk_lower]
        update_set = ', '.join([f"{c} = EXCLUDED.{c}" for c in update_cols])
        
        insert_sql = f"INSERT INTO {dst_table} ({col_list}) VALUES ({placeholders})"
        check_sql = f"SELECT {col_list} FROM {dst_table} WHERE {pk_col} = %s"
        update_sql = f"UPDATE {dst_table} SET {update_set} WHERE {pk_col} = %s"
        
        inserts = updates = offset = 0
        
        while True:
            cur_src.execute(
                f"SELECT * FROM {src_table} ORDER BY {pk_col} LIMIT {batch_size} OFFSET {offset}"
            )
            rows = cur_src.fetchall()
            if not rows:
                break
            
            for row in rows:
                pk_val = row[cols_lower.index(pk_lower)]
                cur_dst.execute(check_sql, (pk_val,))
                existing = cur_dst.fetchone()
                
                if existing is None:
                    cur_dst.execute(insert_sql, row)
                    inserts += 1
                else:
                    existing_dict = dict(zip(cols_lower, existing))
                    new_dict = dict(zip(cols_lower, row))
                    
                    has_changes = False
                    for col in cols_lower:
                        if col != pk_lower and existing_dict.get(col) != new_dict.get(col):
                            has_changes = True
                            break
                    
                    if has_changes:
                        cur_dst.execute(update_sql, list(row))
                        updates += 1
            
            self.master_db.commit()
            offset += batch_size
            if offset % 10000 == 0:
                logger.info(f"    Processed {offset:,} rows (ins: {inserts:,}, upd: {updates:,})...")
        
        return inserts, updates

    def _sync_queue(self) -> int:
        cur = self.master_db.cursor()
        sql = """
            INSERT INTO interface.sync_queue (table_name, record_pk, operation, sync_status)
            SELECT 'enterprise', EnterpriseNumber,
                   CASE WHEN LastUpdatedVersion = LastSeenVersion THEN 'INSERT' ELSE 'UPDATE' END,
                   'PENDING'
            FROM kbo_master.enterprise
            WHERE EnterpriseNumber NOT IN (
                SELECT record_pk FROM interface.sync_queue WHERE table_name = 'enterprise'
            )
            ON CONFLICT DO NOTHING
        """
        cur.execute(sql)
        queued = cur.rowcount
        self.master_db.commit()
        return queued

    def _update_extract_version(self, version_id: str, snapshot_date: str,
                                 source_folder: str, records_inserted: int,
                                 records_updated: int, records_queued: int):
        cur = self.master_db.cursor()
        sql = """
            INSERT INTO ref.extract_version (
                version_id, snapshot_date, source_country, source_system,
                source_folder, db_name, merge_status, records_inserted,
                records_updated, records_queued, started_at, completed_at
            ) VALUES (
                %s, %s, 'BE', 'KBO', %s, %s, 'DONE',
                %s, %s, %s, NOW(), NOW()
            )
            ON CONFLICT (version_id) DO UPDATE SET
                merge_status = 'DONE',
                records_inserted = EXCLUDED.records_inserted,
                records_updated = EXCLUDED.records_updated,
                records_queued = EXCLUDED.records_queued,
                completed_at = NOW()
        """
        cur.execute(sql, (version_id, snapshot_date, source_folder,
                         f'BE KBO {version_id}',
                         records_inserted, records_updated, records_queued))


def main():
    parser = argparse.ArgumentParser(description='BE KBO Merge to Master')
    parser.add_argument('version_id')
    parser.add_argument('snapshot_date')
    parser.add_argument('versioned_db')
    parser.add_argument('source_folder')
    parser.add_argument('--db-host', default='127.0.0.1')
    parser.add_argument('--db-port', type=int, default=5434)
    parser.add_argument('--db-user', default='aiuser')
    parser.add_argument('--db-password', default='aipassword123')
    
    args = parser.parse_args()
    db_config = DBConfig(host=args.db_host, port=args.db_port, database='postgres',
                         user=args.db_user, password=args.db_password)
    
    merger = KBOMergeToMaster(db_config)
    result = merger.merge(args.version_id, args.snapshot_date,
                         args.versioned_db, args.source_folder)
    
    logger.info(f"Merge complete: {result['records_inserted']:,} inserted, "
                f"{result['records_updated']:,} updated, {result['records_queued']:,} queued")


if __name__ == '__main__':
    main()