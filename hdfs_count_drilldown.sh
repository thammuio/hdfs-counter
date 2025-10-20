#!/bin/bash

# Accept file limit as a parameter, default to 5000000 if not provided
MAX_FILES=${2:-5000000}
printed_header=0
output_file="hdfs_count_output.csv"

drilldown() {
    local dir="$1"
    local found_subdir=0
    local subdirs=()

    # Get counts for immediate children
    hdfs dfs -count -q -h "$dir"/* 2>/dev/null | while read -r line; do
        file_count=$(echo "$line" | awk '{print $6}' | tr -d ',')
        size=$(echo "$line" | awk '{print $7}')
        path=$(echo "$line" | awk '{print $NF}')
        [[ "$file_count" =~ ^[0-9]+$ ]] || continue
        # Only consider directories (not files)
        if hdfs dfs -test -d "$path"; then
            if [ "$file_count" -gt "$MAX_FILES" ]; then
                found_subdir=1
                subdirs+=("$path")
            fi
        fi
    done

    if [ $found_subdir -eq 1 ]; then
        for subdir in "${subdirs[@]}"; do
            drilldown "$subdir"
        done
    else
        # Print directory path, file count and size in GB/TB for the current dir
        line=$(hdfs dfs -count -q -h "$dir" 2>/dev/null | tail -1)
        file_count=$(echo "$line" | awk '{print $6}' | tr -d ',')
        size=$(echo "$line" | awk '{print $7}')
        path=$(echo "$line" | awk '{print $NF}')
        [[ "$file_count" =~ ^[0-9]+$ ]] || return
        if [ $printed_header -eq 0 ]; then
            echo "Directory, Number of Files, Size" > "$output_file"
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
echo "Output written to $output_file"
