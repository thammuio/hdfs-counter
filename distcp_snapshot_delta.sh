#!/bin/bash
# Delta distcp: usage: distcp_snapshot_delta.sh <namenode> <directory> <base_snapshot_name>

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <namenode> <directory>"
    exit 1
fi

SRC_NN="$1"
SRC_PORT="8020"
SRC_FS="hdfs://${SRC_NN}:${SRC_PORT}"
picked_dir="$2"
base_snapshot="$3"
distcp_success_state_for_dir="stats/distcp_state.txt"
old_snapshot=$(grep "^${picked_dir}," "$distcp_success_state_for_dir" | tail -1 | awk -F',' '{print $2}')

completed_distcp_tracker="stats/distcp_delta_tracker.csv"
distcp_success_state_for_dir="stats/distcp_delta_state.txt"
distcp_mismatch="stats/distcp_delta_mismatch.txt"

export HADOOP_HEAPSIZE=8192

timestamp=$(date +%Y%m%d_%H%M%S)
dir_clean=$(echo "$picked_dir" | sed 's:/*$::')
last_two=$(echo "$dir_clean" | awk -F'/' '{print $(NF-1)"_"$NF}')
new_snapshot="${last_two}_delta_snapshot_${timestamp}"
# get old successful snapshot name from 
log_file="distcp_${last_two}_${new_snapshot}.log"

echo "Creating snapshot ${new_snapshot} for ${picked_dir}"
hdfs dfsadmin -fs "$SRC_FS" -allowSnapshot "$picked_dir"
hdfs dfs -fs "$SRC_FS" -createSnapshot "$picked_dir" "$new_snapshot" || { echo "Failed to create snapshot"; exit 2; }



hadoop distcp -update -snapshotDiff "${old_snapshot}" "${new_snapshot}" "${SRC_FS}${picked_dir}" "$picked_dir" 2>&1 | tee "logs/$log_file"
distcp_status=$?


if [ "$distcp_status" -eq 0 ]; then
    distcp_src_count=$(hdfs dfs -count "${SRC_FS}${picked_dir}/.snapshot/${new_snapshot}" 2>/dev/null | awk 'NR==1{print $2}')
    distcp_target_count=$(hdfs dfs -count "$picked_dir" 2>/dev/null | awk 'NR==1{print $2}')
    echo "$timestamp,$picked_dir,$picked_file_count,$new_snapshot,$distcp_status,$distcp_src_count,$distcp_target_count" >> "$completed_distcp_tracker"
    if [ "$distcp_src_count" = "$distcp_target_count" ]; then
        echo "$picked_dir, $new_snapshot, $timestamp" >> "$distcp_success_state_for_dir"
    else
        echo "$timestamp - Distcp succeeded but counts mismatch for $picked_dir (distcp_src_count=$distcp_src_count, distcp_target_count=$distcp_target_count). RERUN." >> "$distcp_mismatch"
    fi
else
    echo "$timestamp - Distcp failed for $picked_dir (status=$distcp_status). RERUN."
fi


# record stats
echo "$(date +%Y%m%d_%H%M%S),${picked_dir},${base_snapshot},${new_snapshot},${distcp_status}" >> stats/distcp_delta_tracker.csv

if [ "$distcp_status" -eq 0 ]; then
    echo "$(date +%Y%m%d_%H%M%S) - Delta distcp succeeded for ${picked_dir}" >> stats/distcp_delta_state.txt
else
    echo "$(date +%Y%m%d_%H%M%S) - Delta distcp failed for ${picked_dir} (status=${distcp_status}). See logs/$log_file" >> stats/distcp_delta_mismatch.txt
fi


exit $distcp_status
