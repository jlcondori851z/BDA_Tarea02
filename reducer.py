# reducer.py
# -*- coding: utf-8 -*-

import sys

current_key = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    # Esperamos: key \t 1
    parts = line.split("\t", 1)
    if len(parts) != 2:
        continue

    key, count_str = parts
    try:
        count = int(count_str)
    except ValueError:
        continue

    if current_key == key:
        current_count += count
    else:
        if current_key is not None:
            file_name, word = current_key.split("|||", 1)
            print(f"{file_name}\t{word}\t{current_count}")
        current_key = key
        current_count = count

# Última clave
if current_key is not None:
    file_name, word = current_key.split("|||", 1)
    print(f"{file_name}\t{word}\t{current_count}")
