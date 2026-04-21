#!/usr/bin/env python3
content = open('/app/main.py').read()
content = content.replace('"host": "crm-integritasmrv-db"', '"host": "127.0.0.1"').replace('"port": 5432', '"port": 15432')
open('/app/main.py','w').write(content)
print("Updated CRM connection to use 127.0.0.1:15432")