#!/bin/bash

# --- Configuration & Iteration Parameters ---
# All parameters are defined as lists. Add more space-separated values
# to any variable to expand the number of experiment combinations.
BETA_VALS="1.0"
BATCH_SIZES="80"
CPU_WEIGHTS="1.0"
DURATION_WEIGHTS="0.5"
AVG_CLUSTER_LOADS="0 0.5"
#SCHEDULERS="sparrow powerOfTwo prequal dodoor"
SCHEDULERS="dodoor sparrow"

# Dataset is constant and not iterated over.
DATA_PATH="deploy/resources/data/azure_data"
MAX_DURATIONS="30 60 300 600"
BRANCH_NAME="main"
REBUILD="false"
STATIC_CONFIG_PATH="~/dodoor/config.conf"
HOST_CONFIG_PATH="~/cloud_lab/host_config.json"
TASK_CONFIG_PATH="~/dodoor/deploy/resources/configuration/generated_config/merged_profiler_config.json"
LOG_DIR_PREFIX="azure"
NUM_REQUESTS=10000
CODE_UPDATE="true"
RUN_EXPERIMENT="true"
EXPERIMENT_TIMEOUT_IN_MIN=30
QPS="10 50 100"

# --- Experiment Execution ---
# Loop through every combination of the parameters defined above.
# The structure matches your original script but uses more descriptive variable names.
echo "Starting experiment runs..."
for load in $AVG_CLUSTER_LOADS; do
  experiment_timout_in_min=$((EXPERIMENT_TIMEOUT_IN_MIN / (1 - load)))
  for max_duration in $MAX_DURATIONS; do
    data_path="${DATA_PATH}/azure_data_${max_duration}"
#    num_request=$((NUM_REQUESTS * max_duration / 30))
    num_request=10000
    experiment_timout_in_min=$((experiment_timout_in_min * max_duration / 30))
    LOG_DIR_PREFIX="${LOG_DIR_PREFIX}_${max_duration}"
    for qps in $QPS; do
      for scheduler in $SCHEDULERS; do
        for beta in $BETA_VALS; do
          for batch in $BATCH_SIZES; do
            for cpu_w in $CPU_WEIGHTS; do
              for duration_w in $DURATION_WEIGHTS; do
                echo "----------------------------------------------------------------------"
                echo "🚀 RUNNING EXP:"
                echo "  SCHEDULER=($scheduler) LOAD=($load) BETA=($beta)"
                echo "  BATCH=($batch) CPU_W=($cpu_w) DURATION_W=($duration_w)"
                echo "  DATA_PATH=($DATA_PATH) BRANCH_NAME=($BRANCH_NAME)"
                echo "  REBUILD=($REBUILD) LOG_DIR_PREFIX=($LOG_DIR_PREFIX) RUN_EXPERIMENT=($RUN_EXPERIMENT)"
                echo "  QPS=($qps) MAX_DURATION=($max_duration)"
                echo "  NUM_REQUESTS=($num_request) timeout_in_sec=($experiment_timout_in_min)"
                echo "----------------------------------------------------------------------"
                # Execute the experiment script with the current combination of parameters.
                # Argument order matches your original script: $l $m $n $o $k $j $DATA_PATH $i
                sh deploy/script/single_exp.sh "$beta" "$batch" "$cpu_w" "$duration_w" "$load" "$data_path" "$scheduler" "$BRANCH_NAME" "$REBUILD" "$LOG_DIR_PREFIX" "$STATIC_CONFIG_PATH" "$HOST_CONFIG_PATH" "$TASK_CONFIG_PATH" "$num_request" "$CODE_UPDATE" "$RUN_EXPERIMENT" "$experiment_timout_in_min" "$qps"
              done
            done
          done
        done
      done
    done
  done
done

echo "✅ All experiment combinations completed."