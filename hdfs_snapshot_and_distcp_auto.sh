#!/bin/bash
# hdfs_snapshot_and_distcp.sh
# Runs on DESTINATION cluster; pulls from SOURCE cluster snapshot.
# Uses YARN RM REST API to gauge DistCp concurrency and running containers.

set -euo pipefail

########################################
# ======= CONFIGURABLE SETTINGS ======= #
########################################

# --- Source HDFS (NameNode or HA logical nameservice) ---
SRC_NN="odcsrcnn01.corp.oncor.com"
SRC_PORT="8020"
SRC_FS="hdfs://${SRC_NN}:${SRC_PORT}"

# --- Destination path mapping (this host's default fs is destination) ---
DEST_PREFIX=""               # e.g., "/replicated" (leave empty to mirror same path)

# --- ResourceManager REST endpoints (HA: list both; we’ll try in order) ---
# Use http(s)://host:8088 (or RM webapp HTTPS port)
RM_ENDPOINTS=(
  "http://rm1.dest.example.com:8088"
  "http://rm2.dest.example.com:8088"
)

# --- DistCp behavior & capacity policy ---
COPY_THREADS=3               # -Ddistcp.copy.threads
FILES_PER_MAPPER=25000       # heuristic: 1 mapper per this many files
MIN_MAPPERS=10               # floor per job

# Capacity guard using RM API:
TOTAL_MAPPER_BUDGET=200      # your max allowed concurrent mappers for DistCp class
MAX_DISTCP_APPS=3          # hard cap on concurrent DistCp jobs (apps) regardless of containers

# Snapshot retention on SOURCE (per directory)
SNAPSHOT_RETAIN=5

# Inventory and state files (keep them persistent)
INVENTORY_FILE="hdfs_count_output_5M_sorted.txt"    # CSV: dir,file_count,..
TRACKER_CSV="distcp_tracker.csv"                    # append run results
SUCCESS_STATE="distcp_state.txt"                    # list of successfully synced dirs

# Logging
LOG_DIR="${LOG_DIR:-.}"

# Memory (optional tuning)
export HADOOP_HEAPSIZE=8192

# Locking (prevent overlap)
LOCKFILE="/tmp/hdfs_snapshot_and_distcp.lock"

# Curl timeouts
CURL_CONN_TIMEOUT=2
CURL_MAX_TIME=5

########################################
# ============ SANITY CHECKS ========== #
########################################

exec 200>"$LOCKFILE"
flock -n 200 || { echo "Another run is in progress. Exiting."; exit 0; }

if [[ ! -f "$INVENTORY_FILE" ]]; then
  echo "Input file $INVENTORY_FILE not found!" >&2
  exit 1
fi

[[ -f "$TRACKER_CSV" ]] || echo "timestamp,directory,file_count,snapshot_name,distcp_status,distcp_src_count,distcp_target_count" > "$TRACKER_CSV"
touch "$SUCCESS_STATE"

########################################
# =============== FUNCS =============== #
########################################

_have_jq() { command -v jq >/dev/null 2>&1; }

# Try RM endpoints until one returns apps JSON (ACTIVE RM)
_rm_api_get() {
  local path="$1"  # e.g., /ws/v1/cluster/apps?states=RUNNING&applicationTypes=MAPREDUCE
  local ep json
  for ep in "${RM_ENDPOINTS[@]}"; do
    json=$(curl -fsS --connect-timeout "$CURL_CONN_TIMEOUT" --max-time "$CURL_MAX_TIME" \
           -H "Accept: application/json" "${ep}${path}" 2>/dev/null || true)
    # Minimal sanity check for JSON content with "apps"
    if [[ -n "$json" && "$json" == *'"apps"'* ]]; then
      echo "$json"
      return 0
    fi
  done
  return 1
}

# Count running DistCp apps via RM API
count_running_distcp_apps_api() {
  local json
  if ! json=$(_rm_api_get "/ws/v1/cluster/apps?states=RUNNING&applicationTypes=MAPREDUCE"); then
    echo "0"
    return 0
  fi

  if _have_jq; then
    echo "$json" | jq '[.apps.app[]? | select((.name|test("(?i)distcp")) and (.state=="RUNNING"))] | length' 2>/dev/null || echo "0"
  else
    # Fallback: crude parse—count RUNNING apps whose "name" contains DistCp/Distcp
    # This is not perfect but works decently for simple payloads.
    echo "$json" \
      | tr '\n' ' ' \
      | sed 's/},{/}\n{/g' \
      | grep -i '"name":"[^"]*distcp' \
      | grep -c '"state":"RUNNING"' || echo "0"
  fi
}

# Sum running containers across running DistCp apps (map-only DistCp => good proxy for mappers)
sum_running_containers_distcp_api() {
  local json
  if ! json=$(_rm_api_get "/ws/v1/cluster/apps?states=RUNNING&applicationTypes=MAPREDUCE"); then
    echo "0"
    return 0
  fi

  if _have_jq; then
    echo "$json" \
      | jq '[.apps.app[]? | select((.name|test("(?i)distcp")) and (.state=="RUNNING")) | (.runningContainers // 0)] | add // 0' 2>/dev/null \
      || echo "0"
  else
    # Fallback: rough extraction of runningContainers fields within DistCp apps
    echo "$json" \
      | tr '\n' ' ' \
      | sed 's/},{/}\n{/g' \
      | awk '
        BEGIN{IGNORECASE=1}
        /"name":"[^"]*distcp"/ && /"state":"RUNNING"/ {
          # try to extract "runningContainers":N
          match($0, /"runningContainers":[ ]*([0-9]+)/, a)
          if (a[1] != "") sum += a[1]
        }
        END{print sum+0}
      '
  fi
}

approx_available_mappers_api() {
  local running_apps running_containers used avail
  running_apps=$(count_running_distcp_apps_api)
  if (( running_apps >= MAX_DISTCP_APPS )); then
    echo "APPS_CAP_EXCEEDED"
    return 0
  fi
  running_containers=$(sum_running_containers_distcp_api)
  # DistCp is map-only; containers ~= running mappers
  used=$(( running_containers ))
  avail=$(( TOTAL_MAPPER_BUDGET - used ))
  (( avail < 0 )) && avail=0
  echo "$avail"
}

best_fit_choice() {
  # Input: available_mappers
  # Output: "dir|mappers|snapshot_name|file_count"
  local available="$1"
  local picked_dir="" picked_mappers=0 snapshot_name="" file_count=0

  while IFS=, read -r dir file_cnt rest; do
    # trim whitespace
    dir="$(echo "${dir:-}" | xargs || true)"
    file_cnt="$(echo "${file_cnt:-}" | xargs || true)"
    [[ -z "$dir" || -z "$file_cnt" ]] && continue
    [[ "$dir" =~ ^# ]] && continue

    # skip if already successful
    grep -Fxq "$dir" "$SUCCESS_STATE" && continue

    # numeric guard
    [[ "$file_cnt" =~ ^[0-9]+$ ]] || continue

    local mappers=$(( (file_cnt + FILES_PER_MAPPER - 1) / FILES_PER_MAPPER ))
    (( mappers < MIN_MAPPERS )) && mappers=$MIN_MAPPERS

    # fit to available (don’t oversubscribe)
    if (( available > 20 )); then
      (( mappers > available )) && continue
    else
      (( mappers > available )) && mappers=$available
      (( mappers < 1 )) && continue
    fi

    # snapshot name
    local dir_clean last_two
    dir_clean=$(echo "$dir" | sed 's:/*$::')
    last_two=$(awk -F'/' '{if (NF>=2) print $(NF-1)"_"$NF; else print $NF}' <<< "$dir_clean")
    snapshot_name="${last_two}_distcp_snapshot_$(date +%Y%m%d_%H%M%S)"

    echo "${dir}|${mappers}|${snapshot_name}|${file_cnt}"
    return 0
  done < "$INVENTORY_FILE"

  return 1
}

create_source_snapshot() {
  local src_dir="$1" snap="$2"
  echo "Ensuring snapshots enabled on SOURCE for $src_dir"
  hdfs dfs -fs "$SRC_FS" -allowSnapshot "$src_dir" 2>/dev/null || true
  echo "Creating snapshot on SOURCE: $snap"
  hdfs dfs -fs "$SRC_FS" -createSnapshot "$src_dir" "$snap"
}

prune_old_snapshots() {
  local src_dir="$1" retain="$2"
  echo "Pruning old snapshots on SOURCE for $src_dir (retain $retain)"
  mapfile -t snaps < <(hdfs dfs -fs "$SRC_FS" -ls "${src_dir%/}/.snapshot" 2>/dev/null \
                      | awk '{print $8}' | sed 's#.*/##' | sort -r)
  local count=${#snaps[@]}
  if (( count > retain )); then
    for s in "${snaps[@]:retain}"; do
      echo "Deleting snapshot $s"
      hdfs dfs -fs "$SRC_FS" -deleteSnapshot "$src_dir" "$s" || true
    done
  fi
}

dst_path_for() {
  local src_dir="$1"
  if [[ -z "$DEST_PREFIX" ]]; then
    echo "$src_dir"
  else
    echo "${DEST_PREFIX%/}/${src_dir#/}"
  fi
}

########################################
# ============== CAPACITY ============= #
########################################

avail=$(approx_available_mappers_api)
if [[ "$avail" == "APPS_CAP_EXCEEDED" ]]; then
  running_apps=$(count_running_distcp_apps_api)
  echo "Too many DistCp jobs running via RM API ($running_apps >= $MAX_DISTCP_APPS). Exiting."
  exit 0
fi

available_mappers=$(( avail + 0 ))
if (( available_mappers < MIN_MAPPERS )); then
  echo "Low capacity via RM API (available_mappers=$available_mappers). Exiting."
  exit 0
fi

choice="$(best_fit_choice "$available_mappers")" || { echo "No eligible directory fits current capacity."; exit 0; }

picked_dir="$(cut -d'|' -f1 <<< "$choice")"
picked_mappers="$(cut -d'|' -f2 <<< "$choice")"
snapshot_name="$(cut -d'|' -f3 <<< "$choice")"
picked_file_count="$(cut -d'|' -f4 <<< "$choice")"

echo "Selected: $picked_dir  mappers=$picked_mappers  avail~$available_mappers"

########################################
# ============== SNAPSHOT ============= #
########################################

create_source_snapshot "$picked_dir" "$snapshot_name"
src_snapshot_path="${picked_dir%/}/.snapshot/${snapshot_name}"   # path relative to source fs

########################################
# ================ COPY =============== #
########################################

SRC_URI="${SRC_FS}${src_snapshot_path}"
DST_PATH="$(dst_path_for "$picked_dir")"

# Ensure destination base exists
hdfs dfs -mkdir -p "$(dirname "$DST_PATH")" || true

# Log file
dir_clean=$(echo "$picked_dir" | sed 's:/*$::')
last_two=$(awk -F'/' '{if (NF>=2) print $(NF-1)"_"$NF; else print $NF}' <<< "$dir_clean")
log_file="${LOG_DIR%/}/distcp_${last_two}_${snapshot_name}.log"

# Pre-count on source snapshot
picked_file_count=$(hdfs dfs -fs "$SRC_FS" -count "$src_snapshot_path" 2>/dev/null | awk 'NR==1{print $2+0}')

echo "Launching DistCp:"
echo "  SRC: $SRC_URI"
echo "  DST: $DST_PATH"
echo "  mappers: $picked_mappers, threads: $COPY_THREADS"
echo "Logs: $log_file"

distcp_opts=(
  -Dmapreduce.map.memory.mb=4096
  -Dmapreduce.map.java.opts=-Xmx3072m
  -Dmapreduce.map.maxattempts=2
  -Ddistcp.copy.threads="$COPY_THREADS"
  -m "$picked_mappers"
  -skipcrccheck
  -strategy dynamic
  -update
  -delete
  -p rbugp
)

# Launch DistCp in background and capture YARN app ID
distcp_output_file="${log_file}.tmp"
hadoop distcp ${distcp_opts[@]} "$SRC_URI" "$DST_PATH" 2>&1 | tee "$distcp_output_file" | tee "$log_file" &
distcp_pid=$!

# Extract YARN application ID from output (wait for it to appear)
app_id=""
for i in {1..30}; do
  app_id=$(grep -Eo 'application_[0-9_]+|application_[0-9]+_[0-9]+' "$distcp_output_file" | head -1)
  if [[ -n "$app_id" ]]; then
    break
  fi
  sleep 2
done

if [[ -z "$app_id" ]]; then
  echo "Could not determine YARN application ID for DistCp. Will fallback to exit code."
  wait $distcp_pid
  distcp_status=$?
else
  echo "DistCp YARN application ID: $app_id"
  # Wait for DistCp process to finish (but don't use its exit code)
  wait $distcp_pid

  # Poll RM API for final app status
  distcp_status="UNKNOWN"
  for ep in "${RM_ENDPOINTS[@]}"; do
    app_json=$(curl -fsS --connect-timeout "$CURL_CONN_TIMEOUT" --max-time "$CURL_MAX_TIME" \
      -H "Accept: application/json" "${ep}/ws/v1/cluster/apps/${app_id}" 2>/dev/null || true)
    if [[ -n "$app_json" && "$app_json" == *'"app"'* ]]; then
      # Try to parse final state
      if _have_jq; then
        final_state=$(echo "$app_json" | jq -r '.app.finalStatus // empty')
      else
        final_state=$(echo "$app_json" | grep -o '"finalStatus":"[^"]*"' | head -1 | sed 's/.*:"//;s/"//')
      fi
      if [[ "$final_state" == "SUCCEEDED" ]]; then
        distcp_status=0
      elif [[ "$final_state" == "FAILED" || "$final_state" == "KILLED" ]]; then
        distcp_status=1
      fi
      break
    fi
  done
  if [[ "$distcp_status" == "UNKNOWN" ]]; then
    # Could not get app status, fallback to exit code
    distcp_status=1
  fi
fi

# Clean up temp output
rm -f "$distcp_output_file"

########################################
# ============ POST-VALIDATE ========== #
########################################

distcp_src_count=$(hdfs dfs -fs "$SRC_FS" -count "$src_snapshot_path" 2>/dev/null | awk 'NR==1{print $2+0}')
distcp_target_count=$(hdfs dfs -count "$DST_PATH" 2>/dev/null | awk 'NR==1{print $2+0}')
timestamp=$(date +%Y-%m-%dT%H:%M:%S)

echo "$timestamp,$picked_dir,$picked_file_count,$snapshot_name,$distcp_status,$distcp_src_count,$distcp_target_count" >> "$TRACKER_CSV"

if [[ "$distcp_status" -eq 0 ]]; then
  echo "DistCp succeeded (src_count=$distcp_src_count, dst_count=$distcp_target_count)"
  if [[ "$distcp_src_count" -eq "$distcp_target_count" ]]; then
    echo "$picked_dir" >> "$SUCCESS_STATE"
  else
    echo "Warning: count mismatch (src=$distcp_src_count dst=$distcp_target_count)."
  fi
  # Don't prune snapshots immediately; wait for later runs
  # prune_old_snapshots "$picked_dir" "$SNAPSHOT_RETAIN"
else
  echo "DistCp failed for $picked_dir (status=$distcp_status). Will retry next run."
fi
