# Dodoor — Artifact Evaluation Module

This directory contains the artifact-evaluation materials for the
Euro-Par 2026 paper *Dodoor: A Cached-Load Scheduler for Heterogeneous
Clusters*. It was prepared for, but **not submitted to**, the Euro-Par
2026 Artifact Evaluation track. It is published here so that anyone
can reproduce the paper's two main quantitative claims on their own
CloudLab allocation.

## What the paper claims

Dodoor is a cluster-task scheduler that maintains a cached view of
every worker's load (piggybacked onto launch RPCs) instead of probing
nodes for load on every task. The paper's two quantitative claims are:

1. **Latency.** At the high-load operating points, dodoor reduces p99
   end-to-end task latency vs. the canonical Power-of-Two-Choices
   baseline by roughly 23 % on a function-bench workload (QPS=400) and
   28 % on an Azure VM-trace workload (QPS=20).
2. **Scheduling overhead.** Dodoor uses approximately one third the
   scheduler↔node RPCs per task that Power-of-Two does (1 launch RPC
   plus amortised piggybacked load updates, vs. 2 probes + 1 launch).

This artifact reproduces both at paper-exact request counts.

## Reviewer-facing entry points

| Document | Purpose |
| --- | --- |
| `README.md` *(this file)* | What the artifact is, the claim mapping, and the AE stage table. |
| `REPRODUCTION.md` | Step-by-step reviewer instructions: provisioning, what to run, what to check. |
| `REPRODUCTION_REPORT.md` | Authors' own re-run of the artifact on a fresh 100-node CloudLab allocation, with measured latency and overhead numbers. |

## Stages: Smoke → Orchestration check → Sampled run → Full run

The artifact ships four stages, ordered cheap to thorough. Reviewers
run them in order; each subsumes the validation of the previous.

| Stage | Command | Wall-clock | Cluster | What it validates |
| --- | --- | --- | --- | --- |
| Smoke | `./smoke.sh` | ≤ 2 min | none | Toolchain (Java 17+, Maven, Python 3.10+); JAR builds; both Java entry points (`ServiceDaemon`, `TaskTracePlayer`) load; analysis Python imports; comparison HTML pipeline renders. |
| Orchestration check | `./small_cluster_test.sh` | ~ 30 min | 5 nodes | Cluster orchestration end-to-end: `parallel-ssh` fan-out, per-host `config_generator`, `collect_logs.py` scp pipeline. All four schedulers produce non-empty metrics. |
| **Sampled run** *(headline AE artifact)* | `./headline_cells.sh both` | **3 h 29 m** (measured on a 100-node allocation) | 100 nodes | Two cells at paper-exact request counts: function @ QPS=400 × 100 000 reqs and azure @ QPS=20 × 4 000 reqs, dodoor + powerOfTwo. Reproduces both quantitative claims of the paper. |
| Full run | `./run.sh` | ~ 39 h | 100 nodes | Paper-exact full sweep: azure {1, 5, 10, 20} × 4 000 reqs; function {100, 200, 300, 400} × 100 000 reqs; tune-batch + tune-duration full 5-point sweeps × 100 000 reqs. Beyond the original AE 8 h budget; provided for full reproducibility. |

## Why a Sampled run, not a reduced-data full sweep?

Two of the schedulers we compare in the paper are partially randomised:
`random` is fully randomised, and Power-of-Two-Choices picks two probes
uniformly at random. If we kept the full QPS sweep but lowered request
counts to fit a budget, the tail-percentile buckets (p99, p999) would
be supported by only a handful of samples each, and **the very ordering
the paper claims could appear inverted in a single trial purely because
of placement luck**.

We therefore prefer two cells at paper-exact request counts (4 000 / 100
000) over a wider sweep at low statistical power. With paper-exact
counts, p99 is estimated from hundreds to thousands of samples and the
ordering is stable across runs.

## Output locations

| Path | Provenance | In git? |
| --- | --- | --- |
| `deploy/plots/` | Reference figures from the paper's run | yes |
| `deploy/resources/log/` | Reference logs from the paper's run (multi-GB) | no — provide separately if needed |
| `deploy/resources/log_ae/` | Logs collected by the reproduction | no — gitignored |
| `deploy/plots_ae/` | Plots regenerated from the reproduction | no — gitignored |
| `deploy/plots_ae/comparison.html` | Side-by-side comparison report | no — gitignored |

The `DODOOR_LOG_BASE_DIR` and `DODOOR_PLOT_BASE_DIR` env vars override
both. Reviewers do not normally need to set them — the orchestrator
scripts already point the AE outputs into `deploy/resources/log_ae/` and
`deploy/plots_ae/` so the reference tree under `deploy/resources/log/`
and `deploy/plots/` is never modified.

## Mapping from experiments to paper figures

| Paper figure | Reference plot file | Producing phase |
| --- | --- | --- |
| Azure VM scheduler performance | `deploy/plots/azure_600/scheduler_performance_figure.png` | `azure` + `plot` |
| Azure VM resource utilisation | `deploy/plots/azure_600/node_metrics_resource_utilization.png` | `azure` + `plot` |
| Azure VM waiting tasks | `deploy/plots/azure_600/node_metrics_waiting_tasks.png` | `azure` + `plot` |
| Function-bench scheduler performance | `deploy/plots/function_100k_100-0-0/scheduler_performance_figure.png` | `function` + `plot` |
| Function-bench resource utilisation | `deploy/plots/function_100k_100-0-0/node_metrics_resource_utilization.png` | `function` + `plot` |
| Function-bench waiting tasks | `deploy/plots/function_100k_100-0-0/node_metrics_waiting_tasks.png` | `function` + `plot` |
| Parameter tune — batch size | `deploy/plots/parameter_tune/function_100k_tune_batch_100-0-0/aggregated_metrics_batch_size.png` | `tune` + `plot` |
| Parameter tune — duration weight | `deploy/plots/parameter_tune/function_100k_tune_duration_weight_100-0-0/aggregated_metrics_duration_weight.png` | `tune` + `plot` |
| Merged CDFs across all experiments | `deploy/plots/parameter_tune/merged_cdf_all_experiments.png` | `plot` |

## Phases (for those running `./run.sh` directly)

| Phase | Action |
| --- | --- |
| `parse` | Parse `manifest.xml` into per-host SSH/IP files and distribute `host_config.json`. |
| `setup` | Clone, build, and install dependencies on every CloudLab node. |
| `function` | Run the function-bench campaign. |
| `azure` | Run the Azure VM placement campaign. |
| `tune` | Run the batch-size and duration-weight parameter sweeps. |
| `plot` | Regenerate every figure from the new logs. |
| `compare` | Build `deploy/plots_ae/comparison.html` from the new plots vs. the reference plots. |

The default order is `parse → setup → function → azure → tune → plot →
compare` — function before azure so the fast (~ 10 min/combo) campaign
acts as a smoke test for the slow (~ 67 min/combo) one.
