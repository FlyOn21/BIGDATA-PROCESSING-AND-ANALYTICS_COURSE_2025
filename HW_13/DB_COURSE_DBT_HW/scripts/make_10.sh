#!/bin/bash
set -e
mkdir -p output

ROWS="${1:-10000}"
if ! [[ "$ROWS" =~ ^[0-9]+$ ]]; then
    echo "Error: The argument must be a positive integer."
    exit 1
fi

for file in *.csv; do
    if [ -f "$file" ]; then
        base_name=$(basename "$file" | cut -d. -f1)
        head -n 10000 "$file" > "output/${base_name}_${ROWS}.csv"
        echo "Processed: $file -> output/${base_name}_${ROWS}.csv"
    fi
done