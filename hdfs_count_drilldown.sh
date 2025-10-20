#!/bin/bash

# Accept file limit as a parameter, default to 5000000 if not provided
MAX_FILES=${2:-5000000}
output_file="hdfs_count_output.csv"

# Print header once at the start
echo "Directory, Number of Files, Size" > "$output_file"

drilldown() {
    dir="$1"

    # List immediate children (dirs and files) into a temp file
    tmpfile=$(mktemp)
    hdfs dfs -ls "$dir" 2>/dev/null | awk '/^[dl-]/{print $NF}' > "$tmpfile"
    
    while IFS= read -r child; do
        # Ignore any path containing /.snapshot/
        case "$child" in
            */.snapshot/*) continue ;;
        esac
        # Only process directories
        hdfs dfs -test -d "$child" || continue

        # Get file count for this child directory
        line=$(hdfs dfs -count -q -h "$child" 2>/dev/null | tail -n 1)
        file_count=$(echo "$line" | awk '{print $6}' | tr -d ',')
        size=$(echo "$line" | awk '{print $7}')
        path=$(echo "$line" | awk '{print $NF}')
        # Ensure file_count is numeric
        case "$file_count" in
            ''|*[!0-9]*) continue ;;
        esac

        if [ "$file_count" -gt "$MAX_FILES" ]; then
            drilldown "$child"
        else
            echo "$path, $file_count, $size" >> "$output_file"
        fi
    done < "$tmpfile"
    
    rm -f "$tmpfile"
}

# Validate input arguments
if [ $# -lt 1 ]; then
    echo "Usage: $0 <hdfs_directory> [max_files]"
    exit 1
fi

# Start the recursive drilldown
drilldown "$1"

echo "Output written to $output_file"
