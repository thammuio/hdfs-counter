#!/bin/bash
set -euo pipefail

# Usage: ./hdfs_count.sh <hdfs_directory> [max_files]
MAX_FILES="${2:-5000000}"
OUTPUT_FILE="hdfs_count_output.csv"

echo "Directory, Number of Files, Size" > "$OUTPUT_FILE"

drilldown() {
  local dir="$1"

  # List immediate children (dirs & files); skip "Found X items" line
  # Extract the last field (path) and iterate
  local tmpfile
  tmpfile="$(mktemp)"
  hdfs dfs -ls "$dir" 2>/dev/null | awk '/^[dl-]/{print $NF}' > "$tmpfile"

  while IFS= read -r child; do
    # Ignore any path containing /.snapshot/
    case "$child" in
      */.snapshot/*) continue ;;
    esac

    # Only process directories
    if ! hdfs dfs -test -d "$child"; then
      continue
    fi

    # Get counts WITHOUT -h (keeps integers and stable columns)
    # Columns with -q are:
    # 1 QUOTA 2 REM_QUOTA 3 SPACE_QUOTA 4 REM_SPACE_QUOTA 5 DIR_COUNT 6 FILE_COUNT 7 CONTENT_SIZE 8 PATHNAME
    local line file_count size path
    line="$(hdfs dfs -count -q "$child" 2>/dev/null | tail -n 1)"
    file_count="$(awk '{print $6}' <<<"$line" | tr -d ',')"
    size="$(awk '{print $7}' <<<"$line")"
    path="$(awk '{print $NF}' <<<"$line")"

    # Ensure file_count is numeric
    case "$file_count" in
      ''|*[!0-9]*) continue ;;
    esac

    if [ "$file_count" -gt "$MAX_FILES" ]; then
      # Too many files here; drill into subdirectories
      drilldown "$child"
    else
      # Leaf (or small enough) — record it
      echo "$path, $file_count, $size" >> "$OUTPUT_FILE"
    fi
  done < "$tmpfile"

  rm -f "$tmpfile"
}

if [ $# -lt 1 ]; then
  echo "Usage: $0 <hdfs_directory> [max_files]"
  exit 1
fi

drilldown "$1"
echo "Output written to $OUTPUT_FILE"
