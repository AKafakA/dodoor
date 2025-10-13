## Data Generation

This guide covers generating traces for Dodoor evaluations. Three datasets are supported via a single entry point:

```
python deploy/python/scripts/generate_data.py --target_qps 10 --num_records 100
```

Unless you set `--target_qps -1` for timeline replay, arrivals follow a Poisson process at the specified QPS. Each dataset has additional options below.

1) Azure Cloud Virtual Machine (VM)

- Source: Microsoft Azure VM trace (SQLite):
  https://github.com/Azure/AzurePublicDataset/blob/master/AzureTracesForPacking2020.md
- The generator converts VM resource ratios into absolute resources using projected host specs.
- Key options include `--projected_host_cores`, `--projected_host_memory`, `--max_cores`, `--max_memory`, and `--max_duration` (ms) to bound requests.

Example:
```
python deploy/python/scripts/generate_data.py \
  --generated_dataset azure \
  --azure_data_path deploy/resources/data/trace_data/azure_trace.sqlite \
  --azure_output_path deploy/resources/data/azure_data \
  --target_qps 10 --num_records 4000 --max_duration 600000
```

2) Function Bench

- Purpose: Evaluate real function execution; tasks run inside Docker with resource constraints.
- Upstream: https://github.com/ddps-lab/serverless-faas-workbench (adapted to run locally and under Docker).
- Use the profiling flow in `configuration-generation.md` to generate the task/type config before creating traces.

Example:
```
python deploy/python/scripts/generate_data.py \
  --generated_dataset function_bench \
  --function_bench_config deploy/resources/configuration/generated_config/merged_profiler_config.json \
  --function_bench_trace_output_path deploy/resources/data/function_bench \
  --target_qps 10 --num_records 20000 \
  --distribution_type gamma --burstiness 1.0 \
  --function_bench_task_mode_distribution 0.6 0.3 0.1
```

3) Huawei Serverless Trace

- Source: https://github.com/sir-lab/data-release/blob/main/README_data_release_2023.md
- Includes 200 function types and per-minute request frequencies. Sample day-1 data is included at `deploy/resources/data/huawei_serverless_trace/`.
- Use `--serverless_num_functions` to restrict to top-K frequent functions.

Example:
```
python deploy/python/scripts/generate_data.py \
  --generated_dataset serverless \
  --serverless_data_dir deploy/resources/data/huawei_serverless_trace \
  --serverless_output_path deploy/resources/data/serverless \
  --target_qps 50 --num_records 100000 --serverless_num_functions 50
```

Note: Portions of the serverless trace have very low function runtimes (sub-millisecond), which may not be faithfully reproducible on commodity hardware without kernel-level isolation and timers. For cluster experiments, prioritize Azure and Function Bench traces.
