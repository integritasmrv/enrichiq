"""
hubspot-webhook-receiver.py
Stdlib only: http.server + urllib. Receives HubSpot webhooks and triggers Windmill.
Runs on server2:5000.
"""
import os, json, logging, threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import urlopen, Request
from urllib.error import HTTPError

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger(__name__)

WINDMILL_URL = os.environ.get("WINDMILL_URL", "http://144.91.126.111:8002")
WINDMILL_TOKEN = os.environ.get("WINDMILL_TOKEN", "")
PORT = int(os.environ.get("PORT", 5000))

BUSINESS_KEY_MAP = {
    "CONTACT": "integritasmrv",
    "COMPANY": "integritasmrv",
}


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != '/webhook/hubspot':
            self.send_error(404, "Not Found")
            return

        try:
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8')
            payload = json.loads(body)
        except Exception as e:
            log.error(f"Failed to parse payload: {e}")
            self.send_error(400, "Invalid JSON")
            return

        log.info(f"HubSpot webhook: {json.dumps(payload, indent=2)[:500]}")

        objects = payload.get('objects', [payload]) if isinstance(payload, dict) else payload
        if not isinstance(objects, list):
            objects = [objects]

        results = []
        for obj in objects:
            obj_type = obj.get('objectType', obj.get('object_type', 'unknown')).upper()
            hs_id = obj.get('objectId', obj.get('id', ''))
            props = obj.get('properties', {})
            company = props.get('company', props.get('company_name', ''))
            firstname = props.get('firstname', '')
            lastname = props.get('lastname', '')
            email = props.get('email', '')
            label = f"{firstname} {lastname}".strip() or company or f"HubSpot {obj_type}"

            business_key = BUSINESS_KEY_MAP.get(obj_type, 'integritasmrv')
            entity_type = 'contact' if obj_type == 'CONTACT' else 'company'

            result = call_windmill(hs_id or email, business_key, entity_type, label)
            results.append(result)

        response = {'status': 'ok', 'processed': len(results), 'results': results}
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(response).encode())

    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"status": "ok"}')
        else:
            self.send_error(404)

    def log_message(self, fmt, *args):
        log.info(fmt % args)


def call_windmill(entity_id, business_key, entity_type, label):
    if not WINDMILL_TOKEN:
        log.warning("WINDMILL_TOKEN not set, skipping")
        return {'entity_id': entity_id, 'windmill': 'skipped'}

    url = f"{WINDMILL_URL}/api/mcp/w/admins/scripts/u/admin/enrich-trigger/run"
    payload = json.dumps({
        'entity_id': entity_id,
        'business_key': business_key,
        'entity_type': entity_type,
        'label': label,
        'source_system': 'hubspot-webhook',
    }).encode()

    try:
        req = Request(url, data=payload, headers={
            'Authorization': f'Bearer {WINDMILL_TOKEN}',
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        })
        resp = urlopen(req, timeout=60)
        result = json.loads(resp.read().decode())
        log.info(f"Windmill triggered: {result}")
        return {'entity_id': entity_id, 'windmill': result}
    except HTTPError as e:
        body = e.read().decode() if e.fp else str(e)
        log.error(f"Windmill HTTP error {e.code}: {body[:200]}")
        return {'entity_id': entity_id, 'windmill': f'error {e.code}'}
    except Exception as e:
        log.error(f"Windmill call failed: {e}")
        return {'entity_id': entity_id, 'windmill': f'error: {e}'}


if __name__ == '__main__':
    server = HTTPServer(('0.0.0.0', PORT), Handler)
    log.info(f"HubSpot webhook receiver listening on port {PORT}")
    server.serve_forever()
