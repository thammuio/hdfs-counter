#!/bin/bash

input_file="hdfs_count_output.csv"
# Use a timestamped snapshot name for uniqueness and delta tracking
snapshot_name="distcp_snapshot_$(date +%Y%m%d_%H%M%S)"
inventory_file="distcp_inventory_$(date +%Y%m%d_%H%M%S).csv"

if [ ! -f "$input_file" ]; then
    echo "Input file $input_file not found!"
    exit 1
fi

# Write inventory header
echo "timestamp,directory,src_count,dest_count" > "$inventory_file"

# Skip header and process each directory
tail -n +2 "$input_file" | while IFS=, read -r dir _; do
    dir=$(echo "$dir" | xargs) # trim whitespace
    if [ -z "$dir" ]; then
        continue
    fi

    # Create snapshot
    echo "Creating snapshot for $dir"
    hdfs dfs -allowSnapshot "$dir"
    hdfs dfs -createSnapshot "$dir" "$snapshot_name"

    # Source and target for distcp
    src="${dir}/.snapshot/${snapshot_name}"
    tgt="${dir}"

    # Print distcp command
    echo "hadoop distcp -Dmapreduce.map.memory.mb=4096 -Dmapreduce.map.java.opts='-Xmx3072m -XX:+UseG1GC' -Dmapreduce.map.maxattempts=2 -m 200 -strategy dynamic -update -delete -p $src $tgt"

    # Run distcp
    hadoop distcp -Dmapreduce.map.memory.mb=4096 -Dmapreduce.map.java.opts='-Xmx3072m -XX:+UseG1GC' -Dmapreduce.map.maxattempts=2 -m 200 -strategy dynamic -update -delete -p "$src" "$tgt"

    # Get counts after copy
    src_count=$(hdfs dfs -count "$src" 2>/dev/null | awk 'NR==1{print $2}')
    dest_count=$(hdfs dfs -count "$tgt" 2>/dev/null | awk 'NR==1{print $2}')
    timestamp=$(date +%Y-%m-%dT%H:%M:%S)

    # Write to inventory
    echo "$timestamp,$dir,$src_count,$dest_count" >> "$inventory_file"
done
