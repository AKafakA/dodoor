#!/bin/bash

# --- Configuration & Iteration Parameters ---
# All parameters are defined as lists. Add more space-separated values
# to any variable to expand the number of experiment combinations.
BETA_VALS="1.0"
BATCH_SIZES="50"
CPU_WEIGHTS="1.0"
DURATION_WEIGHTS="0.5"
SCHEDULERS="powerOfTwo prequal dodoor random"
#SCHEDULERS="dodoor"
LOG_LEVEL="${LOG_LEVEL:-info}"
#SCHEDULERS="sparrow dodoor"

# Dataset is constant and not iterated over.
DATA_PATH="deploy/resources/data/function_bench"
TASK_DISTRIBUTION="100-0-0"
BRANCH_NAME="traceback"
REBUILD="false"
STATIC_CONFIG_PATH="~/dodoor/config.conf"
HOST_CONFIG_PATH="~/cloud_lab/host_config.json"
#HOST_CONFIG_PATH="~/dodoor/deploy/resources/host_addresses/cloud_lab/mask_host_config.json"
TASK_CONFIG_PATH="~/dodoor/deploy/resources/configuration/generated_config/merged_profiler_config.json"
LOG_DIR_PREFIX="function_100k"
NUM_REQUESTS=100000
CODE_UPDATE="false"
RUN_EXPERIMENT="true"
EXPERIMENT_TIMEOUT_IN_MIN=40
QPS="100 200 300 400"
MAX_DURATIONS="600"
#QPS="30"
#MAX_DURATIONS="30"
RESTRICT_FIFO="true"
ENABLE_BACKGROUND_QUERY="false"

RUN_WARMUP="true"
WARMUP_REQUESTS=100
WARMUP_QPS=5
WARMUP_TRACE="${DATA_PATH}/warmup"

ENABLE_PER_TASK_LOGS="false"

# --- Experiment Execution ---
# Loop through every combination of the parameters defined above.
# The structure matches your original script but uses more descriptive variable names.
START_TIME=$(date +%s)
echo "Starting experiment runs..."
for restrict_fifo in $RESTRICT_FIFO; do
  experiment_timout_in_min=$EXPERIMENT_TIMEOUT_IN_MIN
  for qps in $QPS; do
      for max_duration in $MAX_DURATIONS; do
        for task_dist in $TASK_DISTRIBUTION; do
          data_path="${DATA_PATH}/${task_dist}.csv"
          experiment_timout_in_min=$((experiment_timout_in_min * 600 / 600))
          log_dir_prefix="${LOG_DIR_PREFIX}_${task_dist}"
          for scheduler in $SCHEDULERS; do
            for beta in $BETA_VALS; do
              for batch in $BATCH_SIZES; do
                for cpu_w in $CPU_WEIGHTS; do
                  for duration_w in $DURATION_WEIGHTS; do
                    echo "----------------------------------------------------------------------"
                    echo "🚀 RUNNING EXP:"
                    echo "  SCHEDULER=($scheduler) BETA=($beta)"
                    echo "  BATCH=($batch) CPU_W=($cpu_w) DURATION_W=($duration_w)"
                    echo "  DATA_PATH=($DATA_PATH) BRANCH_NAME=($BRANCH_NAME)"
                    echo "  REBUILD=($REBUILD) LOG_DIR_PREFIX=($log_dir_prefix) RUN_EXPERIMENT=($RUN_EXPERIMENT)"
                    echo "  QPS=($qps) TASK_DISTRIBUTION=($task_dist)"
                    echo "  NUM_REQUESTS=($NUM_REQUESTS) timeout_in_sec=($experiment_timout_in_min)"
                    echo "  CODE_UPDATE=($CODE_UPDATE) RESTRICT_FIFO=($RESTRICT_FIFO)"
                    echo "----------------------------------------------------------------------"
                      # Execute the experiment script with the current combination of parameters.
                      # Argument order matches your original script: $l $m $n $o $k $j $DATA_PATH $i
                    sh deploy/script/single_exp.sh "$beta" "$batch" "$cpu_w" "$duration_w" "$data_path" "$scheduler" "$BRANCH_NAME" "$REBUILD" "$log_dir_prefix" "$STATIC_CONFIG_PATH" "$HOST_CONFIG_PATH" "$TASK_CONFIG_PATH" "$NUM_REQUESTS" "$CODE_UPDATE" "$RUN_EXPERIMENT" "$experiment_timout_in_min" "$qps" "$restrict_fifo" "$ENABLE_BACKGROUND_QUERY" "false" "$LOG_LEVEL" "$RUN_WARMUP" "$WARMUP_REQUESTS" "$WARMUP_QPS" "$WARMUP_TRACE" "$ENABLE_PER_TASK_LOGS"
                    done
                done
              done
            done
          done
        done
      done
  done
done

END_TIME=$(date +%s)
ELAPSED_TIME=$((END_TIME - START_TIME))
echo "⏱️ Total elapsed time: $ELAPSED_TIME seconds."
echo "✅ All experiment combinations completed."
