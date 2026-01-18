# reducer.py
# -*- coding: utf-8 -*-

import sys

current_key = None
current_count = 0

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    # Esperamos: fichero \t palabra \t 1
    parts = line.split("\t")
    if len(parts) != 3:
        continue

    file_name, word, count_str = parts
    key = f"{file_name}\t{word}"

    try:
        count = int(count_str)
    except ValueError:
        continue

    # Hadoop ordena por clave antes de llegar al reducer
    if current_key == key:
        current_count += count
    else:
        if current_key is not None:
            print(f"{current_key}\t{current_count}")
        current_key = key
        current_count = count

# Última clave
if current_key is not None:
    print(f"{current_key}\t{current_count}")
