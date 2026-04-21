import asyncpg

@app.post("/webhook/hubspot")
async def webhook_hubspot(request: Request):
    try:
        body = await request.json()
        events = body if isinstance(body, list) else [body]
        print(f"[WEBHOOK] HubSpot: {len(events)} event(s)", flush=True)
        
        results = []
        
        for evt in events:
            try:
                props = evt.get("properties", {})
                ot = evt.get("objectType", "contact").lower()
                hs_id = str(evt.get("objectId", ""))
                crm_name = "integritasmrv"
                
                cfg = {
                    "host": "10.0.13.2",
                    "port": 5432,
                    "user": "integritasmrv_crm_user",
                    "password": "oYxxPKRfAHAD263VSDcKmljKY0vInx2QTl6PooKoqmmiDops",
                    "database": "integritasmrv_crm",
                    "ssl": None
                }
                
                conn = await asyncpg.connect(**cfg)
                
                if "company" in ot:
                    nv = props.get("name", props.get("company", {}))
                    name = nv.get("value", "") if isinstance(nv, dict) else str(nv or "")
                    if not name:
                        name = "Company " + hs_id
                    
                    ex = await conn.fetchrow("SELECT id FROM nb_crm_customers WHERE hubspot_id = $1", hs_id)
                    if ex:
                        await conn.execute('UPDATE nb_crm_customers SET enrichment_status = $2, "updatedAt" = NOW() WHERE hubspot_id = $1', hs_id, "To Be Enriched")
                        results.append({"action": "updated", "crm": crm_name, "type": "company", "id": ex["id"]})
                    else:
                        row = await conn.fetchrow('INSERT INTO nb_crm_customers (name, hubspot_id, enrichment_status, "updatedAt", "createdAt") VALUES ($1, $2, $3, NOW(), NOW()) RETURNING id', name, hs_id, "To Be Enriched")
                        results.append({"action": "created", "crm": crm_name, "type": "company", "id": row["id"] if row else None})
                else:
                    fv = props.get("firstname", {})
                    lv = props.get("lastname", {})
                    fn2 = fv.get("value", "") if isinstance(fv, dict) else str(fv or "")
                    ln2 = lv.get("value", "") if isinstance(lv, dict) else str(lv or "")
                    full = (fn2 + " " + ln2).strip() or "Unknown"
                    ev2 = props.get("email", {})
                    email = ev2.get("value", "") if isinstance(ev2, dict) else str(ev2 or "")
                    
                    ex = await conn.fetchrow("SELECT id FROM nb_crm_contacts WHERE hubspot_id = $1", hs_id)
                    if ex:
                        await conn.execute('UPDATE nb_crm_contacts SET enrichment_status = $2, "updatedAt" = NOW() WHERE hubspot_id = $1', hs_id, "To Be Enriched")
                        results.append({"action": "updated", "crm": crm_name, "type": "contact", "id": ex["id"]})
                    else:
                        row = await conn.fetchrow('INSERT INTO nb_crm_contacts (name, email, hubspot_id, enrichment_status, "updatedAt", "createdAt") VALUES ($1, $2, $3, $4, NOW(), NOW()) RETURNING id', full, email or None, hs_id, "To Be Enriched")
                        results.append({"action": "created", "crm": crm_name, "type": "contact", "id": row["id"] if row else None})
                
                await conn.close()
                
            except Exception as ee:
                print(f"[WEBHOOK] Event error: {ee}")
                results.append({"error": str(ee)[:100]})
        
        return {"status": "processed", "results": results}
        
    except Exception as e:
        print(f"HubSpot webhook error: {e}")
        return {"status": "error", "detail": str(e)}