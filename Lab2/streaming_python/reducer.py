#!/usr/bin/env python3
import sys

current_word = None
current_count = 0

for line in sys.stdin:
    line = line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t", 1)   # on attend "mot<TAB>count"
    if len(parts) != 2:
        continue                  # ignore les lignes mal formées

    word, count = parts
    try:
        count = int(count)
    except ValueError:
        continue                  # ignore si count n'est pas un entier

    if current_word is None:
        # première ligne valide
        current_word = word
        current_count = count
        continue

    if word == current_word:
        current_count += count
    else:
        print(f"{current_word}\t{current_count}")
        current_word = word
        current_count = count

# flush final
if current_word is not None:
    print(f"{current_word}\t{current_count}")
