#!/usr/bin/env python3
for fname in ['/app/api_main.py', '/app/main.py', '/app/main_patched.py']:
    try:
        with open(fname, 'r') as f:
            content = f.read()
        target = 'async def webhook_hubspot(request: Request):'
        decorator = '@app.post("/webhook/hubspot")\n'
        if target in content:
            content = content.replace(target, decorator + target)
            with open('/tmp/fixed.py', 'w') as f:
                f.write(content)
            print(f"Fixed: {fname}")
            break
        else:
            print(f"Not in: {fname}")
    except Exception as e:
        print(f"Error reading {fname}: {e}")