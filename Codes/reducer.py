#!/usr/bin/env python3

import sys

current_key = None
sum_prices = 0
total_market_cap = 0
count = 0

for line in sys.stdin:
    try:
        # Parse key, price, and market_cap from the line
        key, price, market_cap = line.strip().split('\t')
        price = float(price)
        market_cap = float(market_cap)

        # Aggregate data by key
        if current_key == key:
            sum_prices += price
            total_market_cap += market_cap
            count += 1
        else:
            if current_key:
                # Emit the average price and total market capitalization for the current key
                print(f"{current_key}\t{sum_prices / count:.2f}\t{total_market_cap:.2f}")
            current_key = key
            sum_prices = price
            total_market_cap = market_cap
            count = 1
    except ValueError as e:
        # Log parsing errors
        sys.stderr.write(f"ValueError: {line.strip()} - {e}\n")

# Output the last key
if current_key:
    print(f"{current_key}\t{sum_prices / count:.2f}\t{total_market_cap:.2f}")
