#!/usr/bin/env python3
with open('/tmp/api_main_new.py', 'r') as f:
    content = f.read()

target = 'async def webhook_hubspot(request: Request):'
decorator = '@app.post("/webhook/hubspot")\n'

if target in content:
    content = content.replace(target, decorator + target)
    print("Decorator added")
else:
    print("Target not found")

with open('/tmp/api_main_fixed3.py', 'w') as f:
    f.write(content)
print(f"Written: {len(content)} bytes")

try:
    compile(content, '/tmp/api_main_fixed3.py', 'exec')
    print("Syntax OK")
except SyntaxError as e:
    print(f"Syntax error: {e}")
