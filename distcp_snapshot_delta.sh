#!/bin/bash
# Delta distcp: usage: distcp_snapshot_delta.sh <namenode> <directory> <base_snapshot_name>

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <namenode> <directory> <base_snapshot_name>"
    exit 1
fi

SRC_NN="$1"
SRC_PORT="8020"
SRC_FS="hdfs://${SRC_NN}:${SRC_PORT}"
picked_dir="$2"
base_snapshot="$3"
distcp_success_state_for_dir="stats/distcp_state.txt"
old_snapshot=$(grep "^${picked_dir}," "$distcp_success_state_for_dir" | tail -1 | awk -F',' '{print $2}')

export HADOOP_HEAPSIZE=8192

timestamp=$(date +%Y%m%d_%H%M%S)
dir_clean=$(echo "$picked_dir" | sed 's:/*$::')
last_two=$(echo "$dir_clean" | awk -F'/' '{print $(NF-1)"_"$NF}')
new_snapshot="${last_two}_delta_snapshot_${timestamp}"
# get old successful snapshot name from 


echo "Creating snapshot ${new_snapshot} for ${picked_dir}"
hdfs dfsadmin -fs "$SRC_FS" -allowSnapshot "$picked_dir"
hdfs dfs -fs "$SRC_FS" -createSnapshot "$picked_dir" "$new_snapshot" || { echo "Failed to create snapshot"; exit 2; }



hadoop distcp -update -snapshotDiff "${old_snapshot}" "${new_snapshot}" "${SRC_FS}${picked_dir}" "$tgt" 2>&1 | tee "logs/$log_file"
distcp_status=$?


# record stats
echo "$(date +%Y%m%d_%H%M%S),${picked_dir},${base_snapshot},${new_snapshot},${distcp_status}" >> stats/distcp_delta_tracker.csv

if [ "$distcp_status" -eq 0 ]; then
    echo "$(date +%Y%m%d_%H%M%S) - Delta distcp succeeded for ${picked_dir}" >> stats/distcp_delta_state.txt
else
    echo "$(date +%Y%m%d_%H%M%S) - Delta distcp failed for ${picked_dir} (status=${distcp_status}). See logs/$log_file" >> stats/distcp_delta_mismatch.txt
fi


exit $distcp_status
