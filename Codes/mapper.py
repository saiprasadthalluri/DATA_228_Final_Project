import json
import sys

def mapper():
    try:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue  # Skip empty lines
            
            try:
                record = json.loads(line)  # Attempt to parse JSON
            except json.JSONDecodeError as e:
                print(f"Invalid JSON line: {line}", file=sys.stderr)
                continue  # Skip lines with JSON parsing errors

            # Check if the required keys exist in the JSON object
            try:
                crypto_id = record['id']
                current_price = record['current_price']
                market_cap = record['market_cap']
            except KeyError as e:
                print(f"Missing key {e} in record: {record}", file=sys.stderr)
                continue  # Skip records missing expected keys
            
            # Validate that values are of the correct type
            if not isinstance(crypto_id, str) or not isinstance(current_price, (int, float)) or not isinstance(market_cap, (int, float)):
                print(f"Invalid data types in record: {record}", file=sys.stderr)
                continue  # Skip records with incorrect data types

            # Emit the desired output
            print(f"{crypto_id}\t{current_price}\t{market_cap}")
    
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)

if __name__ == "__main__":
    mapper()
