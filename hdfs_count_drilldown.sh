#!/bin/bash

# Accept file limit as a parameter, default to 5000000 if not provided
MAX_FILES=${2:-5000000}
printed_header=0
output_file="hdfs_count_output.csv"

drilldown() {
    local dir="$1"
    local found_subdir=0
    # Get counts for immediate children
    mapfile -t lines < <(hdfs dfs -count -q -h "$dir"/* 2>/dev/null)
    for line in "${lines[@]}"; do
        # Extract file count, size, and path
        file_count=$(echo "$line" | awk '{print $6}' | tr -d ',')
        size=$(echo "$line" | awk '{print $7}')
        path=$(echo "$line" | awk '{print $NF}')
        # Skip if file_count is not a number
        [[ "$file_count" =~ ^[0-9]+$ ]] || continue
        if [ "$file_count" -gt "$MAX_FILES" ]; then
            found_subdir=1
            drilldown "$path"
        fi
    done

    # If no subdir was over MAX_FILES, print this dir's info
    if [ $found_subdir -eq 0 ]; then
        # Get this dir's count and size
        line=$(hdfs dfs -count -q -h "$dir" 2>/dev/null | tail -1)
        file_count=$(echo "$line" | awk '{print $6}' | tr -d ',')
        size=$(echo "$line" | awk '{print $7}')
        path=$(echo "$line" | awk '{print $NF}')
        [[ "$file_count" =~ ^[0-9]+$ ]] || return
        if [ $printed_header -eq 0 ]; then
            echo "Directory, Number of Files, Size in GB/TB" > "$output_file"
            printed_header=1
        fi
        echo "$path, $file_count, $size" >> "$output_file"
    fi
}

if [ $# -lt 1 ]; then
    echo "Usage: $0 <hdfs_directory> [max_files]"
    exit 1
fi

drilldown "$1"
echo "Output written to $output_file"
