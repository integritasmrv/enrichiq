import httpx

def main(entity_id: str, hubspot_token: str):
    h = {"Authorization": f"Bearer {hubspot_token}"}
    r = httpx.get(f"http://localhost:8088/api/enrichment/entity/{entity_id}/trusted", headers=h, timeout=30)
    d = r.json()
    hs = httpx.post("https://api.hubapi.com/crm/v3/objects/contacts", headers=h, json={"properties": d}, timeout=30)
    return {"status": "synced"}
