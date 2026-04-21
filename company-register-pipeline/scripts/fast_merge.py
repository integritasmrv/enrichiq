#!/usr/bin/env python3
"""Fast BE KBO Merge using bulk INSERT ON CONFLICT DO UPDATE"""
import sys, logging, psycopg2
sys.path.insert(0, '/opt/integritasmrv/company-register-pipeline')
from common.db_utils import DBConfig
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

class FastMerge:
    def __init__(self):
        self.c = DBConfig(host="127.0.0.1", port=5434)
        self.m = None

    def cm(self):
        self.m = psycopg2.connect(host=self.c.host, port=self.c.port, database="BE KBO MASTER", user=self.c.user, password=self.c.password)
        self.m.autocommit = False

    def cv(self, n):
        return psycopg2.connect(host=self.c.host, port=self.c.port, database=n, user=self.c.user, password=self.c.password)

    def mg(self, vid, sdt, vdn, sfl):
        logger.info("FAST %s", vid)
        self.cm()
        vd = self.cv(vdn)
        for t, p in [("enterprise","EnterpriseNumber"), ("establishment","EstablishmentNumber"), ("denomination","EntityNumber"), ("address","EntityNumber"), ("contact","EntityNumber"), ("activity","EntityNumber"), ("branch","Id"), ("code","EntityNumber")]:
            self._mt(t, p, vd)
        self.m.commit()
        self.m.close()
        vd.close()

    def _mt(self, t, p, vd, batch=5000):
        cs = vd.cursor()
        cd = self.m.cursor()
        s = "kbo." + t
        d = "kbo_master." + t
        cs.execute("SELECT * FROM " + s + " LIMIT 1")
        cols = [x[0].lower() for x in cs.description]
        pl = p.lower()
        uc = [c for c in cols if c != pl]
        us = ", ".join([c + "=EXCLUDED." + c for c in uc])
        cl = ", ".join(cols)
        ph = ", ".join(["%s"] * len(cols))
        sql = "INSERT INTO " + d + " (" + cl + ") VALUES (" + ph + ") ON CONFLICT (" + p + ") DO UPDATE SET " + us
        tot = off = 0
        while True:
            cs.execute("SELECT * FROM " + s + " ORDER BY " + p + " LIMIT " + str(batch) + " OFFSET " + str(off))
            rows = cs.fetchall()
            if not rows: break
            try:
                cd.executemany(sql, rows)
                self.m.commit()
                tot += len(rows)
            except Exception as e:
                self.m.rollback()
                for row in rows:
                    try:
                        cd.execute(sql, row)
                        self.m.commit()
                        tot += 1
                    except: break
            off += batch
            if off % 50000 == 0: logger.info("  %s: %d", t, off)
        logger.info("  %s: done (%d)", t, tot)
        return (tot, 0)

FastMerge().mg("276", "2026-04-20", "BE KBO 276", "/mnt/win_data/BE/KBO/ExtractNumber 276")
logger.info("DONE")