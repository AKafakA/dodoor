This guide explains how to run end-to-end experiments on a multi-host testbed (e.g., testbed) and generate plots. Experiments use `ServiceDaemon` to start Scheduler, Data Store, and Node services. The simulator is not used; late-binding is not included in default runs.

Prerequisites

- Generate host/IP configuration per `configuration-generation.md` (manifest parser and host_config.json).
- Build the project (`sh rebuild.sh`) and ensure Java 17, Maven, Thrift, and Docker are installed on all hosts.

Cluster Setup

Run the packaged setup script on all hosts via SSH lists generated from your manifest:
```bash
cd dodoor
sh deploy/script/setup.sh
```
Note: If you hit Maven rate limits, retry later or clear `~/.m2/repository` on the remote host(s).

Experiment Scripts

- Azure VM placement experiments:
  ```bash
  sh deploy/script/end_to_end_exp/azure.sh
  ```
- Function Bench experiments:
  ```bash
  sh deploy/script/end_to_end_exp/function_bench.sh
  ```
- Parameter tuning (batch size, duration weight):
  ```bash
  sh deploy/script/end_to_end_exp/function_bench_tune_batch_size.sh
  sh deploy/script/end_to_end_exp/function_bench_tune_duration_weight.sh
  ```

Each script declares tunable parameters at the top (e.g., `BATCH_SIZES`, `DURATION_WEIGHTS`, `SCHEDULERS`). Supported schedulers: `dodoor`, `powerOfTwo`, `prequal`, `random`.

Single-Run Helper

Use `single_exp.sh` to run one configuration end-to-end (build optional, start services via `ServiceDaemon`, run workload, collect logs):
```bash
sh deploy/script/single_exp.sh \
  <BETA> <BATCH_SIZE> <CPU_WEIGHT> <DURATION_WEIGHT> <DATA_PATH> <SCHEDULER> <BRANCH> <REBUILD> <LOG_PREFIX> \
  <STATIC_CONFIG> <HOST_CONFIG> <TASK_CONFIG> <NUM_REQUESTS> <CODE_UPDATE> <RUN_EXPERIMENT> <TIMEOUT_MIN> <QPS> \
  <RESTRICT_FIFO> <ENABLE_BACKGROUND_QUERY> <DEBUG_LOGS> <LOG_LEVEL> <RUN_WARMUP> <WARMUP_REQUESTS> <WARMUP_QPS> <WARMUP_TRACE>
```

Manual Service Startup (optional)

If you prefer to run services yourself rather than using the scripts, see `README.md` for `ServiceDaemon` examples. Typical pattern:
- Start Node on each worker (`-n true`), and start Scheduler + Data Store on control node (`-s true -d true`).

Plots and Results

- End-to-end plots:
  ```bash
  python deploy/python/analysis/plot_scheduler.py
  python deploy/python/scripts/plot_node.py
  ```
- Parameter tuning plots:
  ```bash
  python deploy/python/analysis/plot_parameter_tune.py
  ```

Output locations:
- End-to-end experiment plots: `deploy/plots/<log_dir_prefix>/`
- Parameter tuning plots: `deploy/plots/parameter_tune/`
