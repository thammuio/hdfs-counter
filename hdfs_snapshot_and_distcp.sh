#!/bin/bash

# Run every 30 minutes
# */30 * * * * hdfs-counter/hdfs_snapshot_and_distcp.sh >> hdfs-counter/cron.log 2>&1

ACTIVE_NN=odcaaslpapp01.corp.oncor.com

input_file="hdfs_count_output_5M_sorted.txt"
# Use a timestamped snapshot name for uniqueness and delta tracking
snapshot_name="distcp_snapshot_$(date +%Y%m%d_%H%M%S)"
# Use a fixed inventory file for persistence
inventory_file="distcp_inventory.csv"
state_file="distcp_state.txt"

export HADOOP_HEAPSIZE=8192

if [ ! -f "$input_file" ]; then
    echo "Input file $input_file not found!"
    exit 1
fi

# Write inventory header if not exists
if [ ! -f "$inventory_file" ]; then
    echo "timestamp,directory,file_count,snapshot_name,distcp_status,src_count,dest_count" > "$inventory_file"
fi

# Create state file if not exists
touch "$state_file"

# Helper: get running distcp mappers
get_running_mappers() {
    yarn application -list 2>/dev/null | grep DISTCP | awk '{print $1}' | while read appid; do
        yarn application -status "$appid" 2>/dev/null | grep -i 'Application-Resource-Usage' | awk -F'=' '{print $2}' | grep -o '[0-9]*' || echo 0
    done | awk '{sum+=$1} END {print sum}'
}

running_mappers=$(get_running_mappers)
available_mappers=$((200 - running_mappers))
if [ "$available_mappers" -le 0 ]; then
    echo "No available mappers (running=$running_mappers, limit=200). Waiting for next cron run."
    exit 0
fi

picked_dir=""
picked_file_count=""
picked_line=""

# Find the next best fit directory for available mappers
while IFS=, read -r dir file_count _; do
    dir=$(echo "$dir" | xargs)
    file_count=$(echo "$file_count" | xargs)
    if [ -z "$dir" ] || [ -z "$file_count" ]; then
        continue
    fi
    # Skip if already processed
    if grep -Fxq "$dir" "$state_file"; then
        continue
    fi
    # Calculate mappers: 1 per 25,000 files, minimum 1
    mappers=$(( (file_count + 24999) / 25000 ))
    if [ "$mappers" -lt 10 ]; then
        mappers=10
    fi
    # Pick if fits in available mappers
    if [ "$mappers" -le "$available_mappers" ]; then
        picked_dir="$dir"
        picked_file_count="$file_count"
        picked_mappers="$mappers"
        break
    fi
done < "$input_file"

if [ -z "$picked_dir" ]; then
    echo "No eligible directory fits in available mappers ($available_mappers). Exiting."
    exit 0
fi

# Launch distcp for picked_dir
echo "Launching distcp for $picked_dir with $picked_mappers mappers (available=$available_mappers)"

# Create snapshot
echo "Creating snapshot for $picked_dir"
hdfs dfs -allowSnapshot "$picked_dir"
hdfs dfs -createSnapshot "$picked_dir" "$snapshot_name"

src="${picked_dir}/.snapshot/${snapshot_name}"
tgt="$picked_dir"

dir_clean=$(echo "$picked_dir" | sed 's:/*$::')
last_two=$(echo "$dir_clean" | awk -F'/' '{print $(NF-1)"_"$NF}')
log_file="distcp_${last_two}_${snapshot_name}.log"

echo "hadoop distcp -Dmapreduce.map.memory.mb=4096 -Dmapreduce.map.java.opts='-Xmx3072m -XX:+UseG1GC' -Dmapreduce.map.maxattempts=2 -Ddistcp.copy.threads=$threads -m $picked_mappers -skipcrccheck -strategy dynamic -update -delete -p hdfs://${ACTIVE_NN}:8020$src $tgt 2>&1 | tee $log_file"
hadoop distcp -Dmapreduce.map.memory.mb=4096 -Dmapreduce.map.java.opts='-Xmx3072m -XX:+UseG1GC' -Dmapreduce.map.maxattempts=2 -Ddistcp.copy.threads=3 -m "$picked_mappers" -skipcrccheck -strategy dynamic -update -delete -p "hdfs://${ACTIVE_NN}:8020$src" "$tgt" 2>&1 | tee "$log_file"
distcp_status=$?

src_count=$(hdfs dfs -count "$src" 2>/dev/null | awk 'NR==1{print $2}')
dest_count=$(hdfs dfs -count "$tgt" 2>/dev/null | awk 'NR==1{print $2}')
timestamp=$(date +%Y-%m-%dT%H:%M:%S)

echo "$timestamp,$picked_dir,$picked_file_count,$snapshot_name,$distcp_status,$src_count,$dest_count" >> "$inventory_file"

# Only mark as processed if distcp succeeded and counts match
if [ "$distcp_status" -eq 0 ] && [ "$src_count" = "$dest_count" ]; then
    echo "$picked_dir" >> "$state_file"
else
    echo "Distcp failed or counts mismatch for $picked_dir (status=$distcp_status, src_count=$src_count, dest_count=$dest_count). Will retry next run."
fi
