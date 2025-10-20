#!/bin/bash

# Accept file limit as a parameter, default to 5000000 if not provided
MAX_FILES=${2:-5000000}
printed_header=0
output_file="hdfs_count_output.csv"

drilldown() {
    local dir="$1"
    local has_large_subdir=0

    # List immediate children (dirs and files)
    hdfs dfs -ls "$dir" 2>/dev/null | awk '{print $8}' | while read -r child; do
        # Ignore .snapshot directories
        [[ "$child" == */.snapshot ]] && continue
        # Only process directories
        hdfs dfs -test -d "$child" || continue

        # Get file count for this child directory
        line=$(hdfs dfs -count -q -h "$child" 2>/dev/null | tail -1)
        file_count=$(echo "$line" | awk '{print $6}' | tr -d ',')
        size=$(echo "$line" | awk '{print $7}')
        path=$(echo "$line" | awk '{print $NF}')
        [[ "$file_count" =~ ^[0-9]+$ ]] || continue

        if [ "$file_count" -gt "$MAX_FILES" ]; then
            has_large_subdir=1
            drilldown "$child"
        else
            # Print header only once
            if [ $printed_header -eq 0 ]; then
                echo "Directory, Number of Files, Size" > "$output_file"
                printed_header=1
            fi
            echo "$path, $file_count, $size" >> "$output_file"
        fi
    done
}

if [ $# -lt 1 ]; then
    echo "Usage: $0 <hdfs_directory> [max_files]"
    exit 1
fi

drilldown "$1"
echo "Output written to $output_file"
        echo "$path, $file_count, $size" >> "$output_file"
    fi
}

if [ $# -lt 1 ]; then
    echo "Usage: $0 <hdfs_directory> [max_files]"
    exit 1
fi

drilldown "$1"
echo "Output written to $output_file"
