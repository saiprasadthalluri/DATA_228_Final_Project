import sys
import json

for line in sys.stdin:
    record = json.loads(line)
    print(f"{record['id']}\t{record['current_price']}")
