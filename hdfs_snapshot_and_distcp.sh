#!/bin/bash

# Usage: sh hdfs_snapshot_and_distcp.sh <directory>


# --- Source HDFS (NameNode or HA logical nameservice) ---
SRC_NN="active.namenode.com"
SRC_PORT="8020"
SRC_FS="hdfs://${SRC_NN}:${SRC_PORT}"

completed_distcp_tracker="distcp_tracker.csv"
distcp_success_state_for_dir="distcp_state.txt"
distcp_mismatch="distcp_mismatch.txt"

export HADOOP_HEAPSIZE=8192

# Remove the logic related to reading from the directory_list_sorted file and instead use a shell argument for the directory name.

# Check if a directory argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <directory>"
    exit 1
fi

picked_dir="$1"
picked_file_count=""

# Define snapshot name
timestamp=$(date +%Y%m%d_%H%M%S)
dir_clean=$(echo "$picked_dir" | sed 's:/*$::')
last_two=$(echo "$dir_clean" | awk -F'/' '{print $(NF-1)"_"$NF}')
snapshot_name="${last_two}_distcp_snapshot_${timestamp}"

# Create snapshot
echo "Creating snapshot for $picked_dir"
hdfs dfsadmin -fs "$SRC_FS" -allowSnapshot "$picked_dir"
hdfs dfs -fs "$SRC_FS" -createSnapshot "$picked_dir" "$snapshot_name"

distcp_src="${picked_dir}/.snapshot/${snapshot_name}"
tgt="$picked_dir"

# Get actual file count from snapshot source
picked_file_count=$(hdfs dfs -count "${SRC_FS}${distcp_src}" 2>/dev/null | awk 'NR==1{print $2}')


# Determine number of mappers based on file count
picked_mappers=$(( (picked_file_count + 24999) / 25000 ))
if [ "$picked_mappers" -lt 10 ]; then
    picked_mappers=10
fi

log_file="distcp_${last_two}_${snapshot_name}.log"

echo "hadoop distcp -Dmapreduce.map.memory.mb=4096 -Dmapreduce.map.java.opts='-Xmx3072m -XX:+UseG1GC' -Dmapreduce.map.maxattempts=2 -Ddistcp.copy.threads=$threads -m $picked_mappers -skipcrccheck -strategy dynamic -update -delete -p ${SRC_FS}${distcp_src} $tgt 2>&1 | tee $log_file"


hadoop distcp -Dmapreduce.map.memory.mb=4096 -Dmapreduce.map.java.opts='-Xmx3072m -XX:+UseG1GC' -Dmapreduce.map.maxattempts=2 -Ddistcp.copy.threads=3 -m "$picked_mappers" -skipcrccheck -strategy dynamic -update -delete -p "${SRC_FS}${distcp_src}" "$tgt" 2>&1 | tee "$log_file"
distcp_status=$?


if [ "$distcp_status" -eq 0 ]; then
    distcp_src_count=$(hdfs dfs -count "${SRC_FS}${distcp_src}" 2>/dev/null | awk 'NR==1{print $2}')
    distcp_target_count=$(hdfs dfs -count "$tgt" 2>/dev/null | awk 'NR==1{print $2}')
    echo "$timestamp,$picked_dir,$picked_file_count,$snapshot_name,$distcp_status,$distcp_src_count,$distcp_target_count" >> "$completed_distcp_tracker"
    if [ "$distcp_src_count" = "$distcp_target_count" ]; then
        echo "$picked_dir, $snapshot_name, $timestamp" >> "$distcp_success_state_for_dir"
    else
        echo "$timestamp - Distcp succeeded but counts mismatch for $picked_dir (distcp_src_count=$distcp_src_count, distcp_target_count=$distcp_target_count). RERUN." >> "$distcp_mismatch"
    fi
else
    echo "$timestamp - Distcp failed for $picked_dir (status=$distcp_status). RERUN."
fi