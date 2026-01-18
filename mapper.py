# mapper.py
# -*- coding: utf-8 -*-

import sys
import io
import re
import os

file_path = (
    os.environ.get("mapreduce_map_input_file")
    or os.environ.get("map_input_file")
    or ""
)
file_name = os.path.basename(file_path) if file_path else "unknown_file"

input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="ignore")

for line in input_stream:
    line = line.strip()
    if not line:
        continue

    # Ignorar cabecera del CSV
    if line.startswith("game_id,type,player_id"):
        continue

    line = line.lower()

    # Tokenización básica
    tokens = re.split(r"[^a-z0-9]+", line)

    for word in tokens:
        if word:
            # Clave única para que Hadoop agrupe bien
            key = f"{file_name}|||{word}"
            print(f"{key}\t1")
