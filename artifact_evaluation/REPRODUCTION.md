# Dodoor — Reviewer Reproduction Instructions

This document is a step-by-step guide for reproducing the Euro-Par 2026
Dodoor paper. Read `README.md` first for the artifact overview and the
mapping from experiments to paper figures.

## Hardware requirements

The paper was run on a 100-node heterogeneous CloudLab Utah allocation:

| Role | Count | Hardware |
| --- | ---: | --- |
| Scheduler / DataStore | 1 | `c6620` (`amd001`) |
| Worker | 40 | `m510` |
| Worker | 25 | `xl170` |
| Worker | 18 | `c6525-25g` |
| Worker | 17 | `c6620` |

The provisioning manifest is shipped as
`deploy/resources/configuration/manifest.xml`. To allocate the same
cluster on CloudLab Utah, use that manifest. OS: Ubuntu 22.04
(CloudLab default image).

A reduced reproduction can be run on a 5-node cluster (1 scheduler + 4
workers) using `./small_cluster_test.sh`; the headline reproduction
(`./headline_cells.sh both`) requires the full 100-node allocation to
exercise heterogeneity.

## Software requirements

Local (control machine, where you invoke the orchestrator scripts):

- bash 4+
- Java 17+
- Maven 3.6+
- Python 3.10+, with `matplotlib`, `pandas`, `seaborn`, `numpy`
- `parallel-ssh`, `parallel-scp` (Debian/Ubuntu: `pssh` package)
- ssh access to all CloudLab nodes (passphrase-less key recommended)

The cluster nodes auto-install everything they need via
`deploy/script/setup.sh` during the `parse + setup` phase.

## Stage 1 — Smoke (≤ 2 minutes, no cluster needed)

```bash
./smoke.sh
```

Validates: toolchain (Java/Maven/Python); JAR builds; `ServiceDaemon`
and `TaskTracePlayer` Java entry points load; Python analysis stack
imports; the side-by-side `comparison.html` pipeline renders.

A green Smoke means the cluster scripts will not fail for build or
toolchain reasons. **If this fails, do not proceed.**

## Stage 2 — Orchestration check (~ 30 minutes, 5-node cluster)

```bash
export CLOUDLAB_USER=<your-cloudlab-id>
./small_cluster_test.sh
```

Truncates `host_addresses/` to 1 scheduler + 4 worker nodes, runs all
four schedulers (`powerOfTwo`, `dodoor`, `prequal`, `random`) at
QPS=20 × 100 reqs. Verifies every combo produces a non-empty metrics
file.

This catches every distributed-pipeline bug class
(`parallel-ssh` fan-out, per-host `config_generator`, `collect_logs.py`
scp pipeline) without paying the full 100-node cost.

A green Orchestration check means the cluster pipeline is healthy and
you can move to the Sampled run.

## Stage 3 — Sampled run (3.5 h, 100-node cluster, AE budget) — *headline*

```bash
export CLOUDLAB_USER=<your-cloudlab-id>
./headline_cells.sh both
```

This runs the two cells in which dodoor's paper claims are strongest,
**at paper-exact request counts**:

- function bench: dodoor + powerOfTwo × QPS=400 × **100 000 reqs**
- azure VM: dodoor + powerOfTwo × QPS=20 × **4 000 reqs**

You can also run them individually:

```bash
./headline_cells.sh function    # function-only,  ~ 1 h
./headline_cells.sh azure       # azure-only,     ~ 2.5 h
```

Wall-clock measured on a fresh 100-node CloudLab allocation: **3 h
29 m** for `both`.

### What to check after Stage 3 finishes

1. Every `tier3*_results/scheduler/<workload>/<combo>/metrics/*.log` is
   non-empty (one log per scheduler + QPS combo).
2. The plots regenerated under `deploy/plots_ae/` match the reference
   plots under `deploy/plots/` qualitatively (same shapes, same scheduler
   ordering at each QPS).
3. Open `deploy/plots_ae/comparison.html` for a side-by-side view of
   reproduction vs. reference, with numeric latency tables.

The expected numeric verdict (from our re-run, see
`REPRODUCTION_REPORT.md` for the full data):

| Cell | Metric | dodoor | powerOfTwo | dodoor wins |
| --- | --- | ---: | ---: | ---: |
| azure QPS=20 (4 000 reqs) | p99 latency | 21.4 ms | 27.4 ms | **22 %** |
| | sched. msgs / task | 1.35 | 3.00 | **−55 %** |
| function QPS=400 (100k reqs) | p99 latency | 4.0 ms | 5.0 ms | **20 %** |
| | sched. msgs / task | 1.35 | 3.00 | **−55 %** |

## Stage 4 — Full run (~ 39 h, paper-exact, optional)

```bash
export CLOUDLAB_USER=<your-cloudlab-id>
./run.sh
```

Paper-exact full sweep:

- function: 4 schedulers × QPS ∈ {100, 200, 300, 400} × 100 000 reqs
- azure: 4 schedulers × QPS ∈ {1, 5, 10, 20} × 4 000 reqs
- tune-batch: dodoor × QPS=100 × batch ∈ {25, 50, 75, 100, 150} × 100 000 reqs
- tune-duration-weight: dodoor × QPS=100 × weight ∈ {0.0, 0.25, 0.5, 0.75, 1.0} × 100 000 reqs

This regenerates every paper figure (not only the headline cells). It
needs a ≥ 48 h CloudLab allocation. The Full run is what we use to
populate `REPRODUCTION_REPORT.md`'s observed-vs-paper tables.

## Selecting a subset of phases

```bash
./run.sh --phase function plot compare        # function bench only + figures
./run.sh --phase parse setup                  # provision the cluster, no campaigns
./run.sh --phase tune plot compare            # parameter sweeps only
```

The first two phases (`parse`, `setup`) only need to run once per
allocation; subsequent invocations can skip them.

## Output locations and the safe-default protection

All scripts route their outputs into a separate tree from the reference:

- New logs: `deploy/resources/log_ae/` (override with `DODOOR_LOG_BASE_DIR`)
- New plots: `deploy/plots_ae/` (override with `DODOOR_PLOT_BASE_DIR`)
- Comparison report: `deploy/plots_ae/comparison.html`

`collect_logs.py` defaults its output base to
`deploy/resources/log_ae/` so that running it directly will not
silently overwrite the shipped reference logs. To intentionally write
into the reference tree, set `DODOOR_LOG_BASE_DIR=deploy/resources/log`
explicitly.

## Troubleshooting

- **`smoke.sh` fails on a missing Python module.** Install with
  `pip install matplotlib pandas seaborn numpy`. The control machine
  must have all four available; the cluster nodes only need
  numerical analysis if you generate plots there.
- **`small_cluster_test.sh` reports "FATAL: config.conf wrong on host
  X".** A `parallel-ssh` reach for that host failed. Check that
  `~/cloud_lab/host_config.json` was distributed during `parse` and
  that `parallel-ssh -h ... uname -a` succeeds for every host listed.
- **A campaign combo writes an empty `metrics/` dir.** The
  scheduler service crashed at startup. Inspect the corresponding
  `.err` file under `deploy/resources/log_ae/scheduler/<combo>/service_log/`
  for the stack trace.
- **`headline_cells.sh` exits with "REFUSING to overwrite reference
  logs".** You explicitly set `DODOOR_LOG_BASE_DIR=deploy/resources/log`
  via env. If that was unintentional, `unset DODOOR_LOG_BASE_DIR`.
