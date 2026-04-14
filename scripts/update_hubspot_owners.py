import urllib.request, json

TOKEN = "<HUBSPOT_TOKEN>"
OWNER_ID = "33593468"
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def get_all_ids(object_type):
    all_ids = []
    after = None
    while True:
        body = {"filterGroups": [], "properties": ["id"], "limit": 100}
        if after:
            body["after"] = after
        data = json.dumps(body).encode()
        req = urllib.request.Request(
            f"https://api.hubapi.com/crm/v3/objects/{object_type}/search",
            data=data, headers=HEADERS, method="POST"
        )
        r = urllib.request.urlopen(req, timeout=30)
        d = json.loads(r.read().decode())
        ids = [r["id"] for r in d.get("results", [])]
        all_ids.extend(ids)
        print(f"  {object_type}: page +{len(ids)}, total {len(all_ids)}")
        paging = d.get("paging", {})
        after = paging.get("next", {}).get("after")
        if not after:
            break
    return all_ids

def batch_update(object_type, ids):
    total = len(ids)
    done = 0
    for i in range(0, total, 100):
        batch = ids[i:i+100]
        inputs = [{"id": bid, "properties": {"hubspot_owner_id": OWNER_ID}} for bid in batch]
        payload = json.dumps({"inputs": inputs}).encode()
        req = urllib.request.Request(
            f"https://api.hubapi.com/crm/v3/objects/{object_type}/batch/update",
            data=payload, headers=HEADERS, method="POST"
        )
        r = urllib.request.urlopen(req, timeout=60)
        result = json.loads(r.read().decode())
        updated = len(result.get("results", []))
        done += updated
        print(f"  {object_type}: updated {done}/{total}")
    return done

for obj_type in ["contacts", "companies"]:
    print(f"\nFetching {obj_type}...")
    ids = get_all_ids(obj_type)
    print(f"Updating {len(ids)} {obj_type}...")
    batch_update(obj_type, ids)
    print(f"  {obj_type} DONE")
