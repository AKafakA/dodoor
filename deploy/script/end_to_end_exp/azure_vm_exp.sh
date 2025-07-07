#!/bin/bash

# --- Configuration & Iteration Parameters ---
# All parameters are defined as lists. Add more space-separated values
# to any variable to expand the number of experiment combinations.
BETA_VALS="1.0"
BATCH_SIZES="100"
CPU_WEIGHTS="1.0"
DURATION_WEIGHTS="1.0"
AVG_CLUSTER_LOADS="0 0.5 0.8 0.9"
SCHEDULERS="sparrow powerOfTwo powerOfTwoDuration prequal random dodoor cachedPowerOfTwo"

# Dataset is constant and not iterated over.
DATA_PATH="resources/data/trace_data/azure_data_cloudlab_1m"
BRANCH_NAME="main"
REBUILD="true"
STATIC_CONFIG_PATH="$HOME/dodoor/config.conf"
HOST_CONFIG_PATH="$HOME/dodoor/resources/host_addresses/cloud_lab/host_config.json"
TASK_CONFIG_PATH="$HOME/dodoor/resources/configuration/function_bench_config.json"
LOG_DIR_PREFIX="azure"
NUM_REQUESTS=1000


# --- Experiment Execution ---
# Loop through every combination of the parameters defined above.
# The structure matches your original script but uses more descriptive variable names.
echo "Starting experiment runs..."

for scheduler in $SCHEDULERS; do
  for load in $AVG_CLUSTER_LOADS; do
    for beta in $BETA_VALS; do
      for batch in $BATCH_SIZES; do
        for cpu_w in $CPU_WEIGHTS; do
          for duration_w in $DURATION_WEIGHTS; do
            echo "----------------------------------------------------------------------"
            echo "🚀 RUNNING EXP:"
            echo "  SCHEDULER=($scheduler) LOAD=($load) BETA=($beta)"
            echo "  BATCH=($batch) CPU_W=($cpu_w) DURATION_W=($duration_w)"
            echo "----------------------------------------------------------------------"
            # Execute the experiment script with the current combination of parameters.
            # Argument order matches your original script: $l $m $n $o $k $j $DATA_PATH $i
            sh deploy/script/single_exp.sh "$beta" "$batch" "$cpu_w" "$duration_w" "$load" "$DATA_PATH" "$scheduler" "$BRANCH_NAME" "$REBUILD" "$LOG_DIR_PREFIX" "$STATIC_CONFIG_PATH" "$HOST_CONFIG_PATH" "$TASK_CONFIG_PATH" "$NUM_REQUESTS"
          done
        done
      done
    done
  done
done

echo "✅ All experiment combinations completed."