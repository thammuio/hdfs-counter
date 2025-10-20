#!/bin/bash

# Accept file limit as a parameter, default to 5000000 if not provided
MAX_FILES=${2:-5000000}
printed_header=0
output_file="hdfs_count_output.csv"

drilldown() {
    local dir="$1"
    # Get counts for immediate children
    hdfs dfs -count -q -h "$dir"/* 2>/dev/null | while read -r line; do
        # Extract file count, size, and path
        file_count=$(echo "$line" | awk '{print $6}' | tr -d ',')
        size=$(echo "$line" | awk '{print $7}')
        path=$(echo "$line" | awk '{print $NF}')
        # Skip if file_count is not a number
        [[ "$file_count" =~ ^[0-9]+$ ]] || continue
        if [ "$file_count" -le "$MAX_FILES" ]; then
            if [ $printed_header -eq 0 ]; then
                echo "Directory, Number of Files, Size in GB/TB" > "$output_file"
                printed_header=1
            fi
            echo "$path, $file_count, $size" >> "$output_file"
        else
            drilldown "$path"
        fi
    done
}

if [ $# -lt 1 ]; then
    echo "Usage: $0 <hdfs_directory> [max_files]"
    exit 1
fi

drilldown "$1"
echo "Output written to $output_file"
