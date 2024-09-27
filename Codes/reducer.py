import sys

current_key = None
sum_values = 0
count = 0

for line in sys.stdin:
    key, value = line.strip().split('\t')
    value = float(value)
    if current_key == key:
        sum_values += value
        count += 1
    else:
        if current_key:
            print(f"{current_key}\t{sum_values / count}")
        current_key = key
        sum_values = value
        count = 1
if current_key:
    print(f"{current_key}\t{sum_values / count}")
