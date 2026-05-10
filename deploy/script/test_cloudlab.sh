#!/bin/bash

SCHEDULER_NUM_TASKS_UPDATE=4
SCHEDULER_TYPE=$1
BATCH_SIZE=$2
BETA=$3
CPU_WEIGHT=$4
NETWORK_INTERFACE="enp1s0"
DATA_PATH=$5
DURATION_WEIGHT=$6
HOST_CONFIG_PATH=$7
TASK_CONFIG_PATH=$8
STATIC_CONFIG_PATH=${9}
NUM_REQUESTS=${10}
RUN_EXPERIMENT=${11}
EXPERIMENT_TIMEOUT_IN_MIN=${12}
QPS=${13}
RESTRICT_FIFO=${14}
ENABLE_BACKGROUND_QUERY=${15}
LOG_LEVEL=${16}

WARMUP=${17}
WARMUP_REQUESTS=${18}
WARMUP_QPS=${19}
WARMUP_TRACE=${20}

ENABLE_PER_TASK_LOGS=${21}

if [ "${ENABLE_BACKGROUND_QUERY}" = "true" ]; then
  REQUEST_TO_SENT=0
else
  REQUEST_TO_SENT=$NUM_REQUESTS
fi

parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "sudo pkill -f dodoor" > /dev/null 2>&1
parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "sudo pkill -f stress" > /dev/null 2>&1
parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "sudo pkill -f docker" > /dev/null 2>&1

# Generate config.conf on every host. -t 0 (unlimited timeout); error
# output kept visible so silent failures (which were corrupting combo 2/3/4
# during earlier runs) surface in the run log.
echo "Generating config.conf on all hosts (scheduler.type=$SCHEDULER_TYPE)..."
parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_host -i  "cd dodoor && sudo python3 deploy/python/scripts/config_generator.py --replay_with_delay True --batch-size $BATCH_SIZE --beta $BETA --scheduler-type $SCHEDULER_TYPE --scheduler-num-tasks-update $SCHEDULER_NUM_TASKS_UPDATE --network_interface $NETWORK_INTERFACE --cpu_weight $CPU_WEIGHT --duration_weight $DURATION_WEIGHT --restrict_fifo $RESTRICT_FIFO --start_tracking_task_makespan_id 0 --end_tracking_task_makespan_id $NUM_REQUESTS --log_per_task_metrics $ENABLE_PER_TASK_LOGS" 2>&1 | tail -3

# Verify config.conf was actually updated on every host. Without this
# check, a silent parallel-ssh failure leaves config.conf with the
# previous combo's scheduler.type — the JVM then runs as the wrong
# scheduler and writes metrics to the wrong filename.
CONF_CHECK_DIR=$(mktemp -d -t dodoor-conf-check-XXXXXX)
parallel-ssh -t 30 -h deploy/resources/host_addresses/cloud_lab/test_host \
  -o "$CONF_CHECK_DIR" \
  "grep '^scheduler.type' ~/dodoor/config.conf" >/dev/null 2>&1
BAD_HOSTS=()
for f in "$CONF_CHECK_DIR"/*; do
  if ! grep -q "^scheduler\\.type *= *${SCHEDULER_TYPE} *$" "$f" 2>/dev/null; then
    BAD_HOSTS+=("$(basename "$f")")
  fi
done
if [ ${#BAD_HOSTS[@]} -gt 0 ]; then
  echo "FATAL: config_generator failed to set scheduler.type=$SCHEDULER_TYPE on ${#BAD_HOSTS[@]} host(s):"
  printf '  %s\n' "${BAD_HOSTS[@]}" | head -10
  rm -rf "$CONF_CHECK_DIR"
  exit 1
fi
rm -rf "$CONF_CHECK_DIR"
echo "  ok  config.conf has scheduler.type=$SCHEDULER_TYPE on all $(wc -l < deploy/resources/host_addresses/cloud_lab/test_host) hosts"

parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_nodes -i "nohup java -Ddodoor.log.level=${LOG_LEVEL} -cp ~/dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.ServiceDaemon -c ${STATIC_CONFIG_PATH} -hc ${HOST_CONFIG_PATH} -tc ${TASK_CONFIG_PATH} -d false -s false -n true  1>${SCHEDULER_TYPE}_node_service.out  2>${SCHEDULER_TYPE}_node_service.err &" > /dev/null 2>&1

sleep 30
parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_scheduler -i "nohup java -Ddodoor.log.level=${LOG_LEVEL} -cp ~/dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.ServiceDaemon -c ${STATIC_CONFIG_PATH} -hc ${HOST_CONFIG_PATH} -tc ${TASK_CONFIG_PATH} -d true -s true -n false  1>${SCHEDULER_TYPE}_scheduler_service.out 2>${SCHEDULER_TYPE}_scheduler_service.err &" > /dev/null 2>&1


if [ "${WARMUP}" = "true" ]; then
  echo "Starting warmup with ${WARMUP_REQUESTS} requests at ${WARMUP_QPS} QPS using trace ${WARMUP_TRACE}"
  parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_scheduler -i "nohup java -cp dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.client.TaskTracePlayer -c ${STATIC_CONFIG_PATH} -hc ${HOST_CONFIG_PATH} -q ${WARMUP_QPS} -f dodoor/$WARMUP_TRACE -n ${WARMUP_REQUESTS} 1>warmup_replay.out 2>warmup_replay.err &" > /dev/null 2>&1
  sleep 120
  echo "Waited 120 seconds for warmup to complete."
fi

if [ "$RUN_EXPERIMENT" = "true" ]; then
  sleep 20
  echo "Starting the experiment run with QPS=${QPS} and NUM_REQUESTS=${NUM_REQUESTS} for scheduler type ${SCHEDULER_TYPE} with data path ${DATA_PATH}."
  parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_scheduler -i "nohup java -cp dodoor/target/dodoor-1.0-SNAPSHOT.jar edu.cam.dodoor.client.TaskTracePlayer -c ${STATIC_CONFIG_PATH} -hc ${HOST_CONFIG_PATH} -q ${QPS} -f dodoor/$DATA_PATH -n ${REQUEST_TO_SENT} 1>${SCHEDULER_TYPE}_replay.out 2>${SCHEDULER_TYPE}_replay.err &" > /dev/null 2>&1
  # Wait for the tasks to complete
  sleep 30

  SCHEDULER_HOSTNAME=$(head -n 1 deploy/resources/host_addresses/cloud_lab/test_scheduler)

  LOG_PATTERN="~/${SCHEDULER_TYPE}_scheduler_metrics_*.log"
  REMOTE_LOG_PATH=$(ssh "${SCHEDULER_HOSTNAME}" "ls -t ${LOG_PATTERN} 2>/dev/null | head -n 1")

  python3 deploy/python/scripts/wait_for_task_completion.py \
      --host "${SCHEDULER_HOSTNAME}" \
      --scheduler_type "${SCHEDULER_TYPE}" \
      --num_requests "${NUM_REQUESTS}" \
      --timeout "${EXPERIMENT_TIMEOUT_IN_MIN}"

  parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  -i "sudo pkill -f dodoor" > /dev/null 2>&1
fi
