# mapper.py
# -*- coding: utf-8 -*-

import sys
import io
import re
import os

# Nombre del fichero que está procesando el mapper (Hadoop Streaming)
# Hadoop lo expone en la variable de entorno: map_input_file
file_path = os.environ.get("map_input_file", "")
file_name = os.path.basename(file_path) if file_path else "unknown_file"

input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8', errors='ignore')

for line in input_stream:
    line = line.strip()
    if not line:
        continue

    # Ignorar cabecera del CSV (para no contar nombres de columnas)
    if line.startswith("game_id,type,player_id"):
        continue

    # Normalización
    line = line.lower()

    # Tokenización básica: separar por cualquier cosa que no sea letra o número
    tokens = re.split(r"[^a-z0-9]+", line)

    for word in tokens:
        if word:
            # Clave = fichero \t palabra
            print(f"{file_name}\t{word}\t1")
