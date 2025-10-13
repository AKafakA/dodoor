# Sparrow Scheduler Fix Notes

## Summary
- Added detailed reservation bookkeeping and confirmation latency tracking to `UnifiedScheduler` so the simulator mirrors late-binding behavior and surfaces sparrow-specific metrics (`_sparrow_confirm_*` counters, reservation fan-out, etc.).
- `SimulationEngine` now passes the event timestamp into `confirm_task_ready`, allowing the scheduler to compute confirmation latency accurately.
- Java `LateBindTaskScheduler` no longer drops reservations when a confirm RPC returns `false`; the reservation stays queued until the scheduler sends a cancel, preventing the loss of HOL work.

## Commits
1. `simulator: align sparrow scheduler reservation tracking`
   - Extends the simulator scheduler with per-task reservation tracking, confirmation metrics, and richer statistics for debugging sparrow behavior.
   - Stores enqueue timestamps per task so confirmation latency can be measured when the handshake completes.
2. `node: keep sparrow reservation when confirm rejected`
   - Updates the Java late-binding scheduler to keep reservations in place after a rejected confirm and only remove them on success, matching the simulator’s expectations.

## Verification Steps
- Build (fails in sandbox due to offline Maven repository):
  ```bash
  mvn -q -DskipTests package
  ```
- Simulator runs used for validation and plot generation:
  ```bash
  PYTHONPATH=. python simulator/run_simulation.py --scheduler sparrow --duration 120 --warmup 0 --workload synthetic --qps 5 --output-dir simulation_runs
  PYTHONPATH=. python simulator/run_simulation.py --scheduler dodoor  --duration 120 --warmup 0 --workload synthetic --qps 5 --output-dir simulation_runs
  ```
- Plot creation (outputs `simulation_runs/plots/scheduler_performance_figure.png`):
  ```bash
  PYTHONPATH=. python deploy/python/analysis/plot_scheduler.py --log_dir simulation_runs --output_dir simulation_runs/plots
  ```

## Observations & Follow-up
- The sparrow run now records confirmation attempts/latencies, but the current configuration still leaves many reservations pending (`missing_reservation` rejections dominate). Additional tuning of the confirmation loop or queue management is needed so long simulations drain the backlog.
- `simulation_runs/sparrow_batch_.../metrics/scheduler.log` captures the new counters for post-run inspection.
- `simulation_runs/plots/` contains the combined comparison figure for reference when iterating on sparrow fixes.

---

# Proposals: Scheduling Improvements + Simulator Fidelity

This section outlines three features to improve fairness and prediction fidelity. Each is guarded by config flags and designed to be opt‑in, preserving current behavior by default.

## 1) Safe FIFO Backfill (optional, starvation‑free)

Goal
- Preserve strict FIFO fairness (no starvation of head heavy tasks) while opportunistically backfilling with short, runnable tasks that will not delay the head.

Config (Java, DodoorConf)
- `dodoor.fifo.backfill.enabled` (default: false)
- `dodoor.fifo.backfill.max.depth` (default: 32) – how far to scan from the head
- `dodoor.fifo.backfill.slack.margin.ms` (default: 0) – safety margin when estimating head earliest runnable time

Java Changes
- File: `src/main/java/edu/cam/dodoor/node/FifoTaskScheduler.java`
  - In `attemptTaskLaunch(...)` when `restrictFifo=true` and head cannot run:
    - If backfill enabled, compute head earliest runnable time (ERT) by simulating resource release from executing tasks and `_numSlots`.
    - Scan the queue up to `max.depth` in order and launch tasks that:
      - Fit current available resources, and
      - Have `duration <= (ERT - now - slack.margin)`.
    - Do not alter head’s position or HOL counters; only clear HOL when head eventually runs.
- File: `src/main/java/edu/cam/dodoor/node/LateBindTaskScheduler.java`
  - In `attemptConfirmNextTaskReadyToRun(...)` when head cannot be confirmed due to resources and `restrictFifo=true`:
    - If backfill enabled, scan pending reservations in order and attempt confirm on the first runnable task whose projected finish ≤ (head ERT − slack). If confirmed, launch; otherwise keep head HOL.

Simulator Changes
- File: `simulator/core/node_executor.py`
  - Add `enable_backfill` + parameters mirroring Java.
  - When HOL cannot run and backfill enabled, apply the same ERT estimation and safe backfill scan.

Validation
- End‑to‑end trace (debug.sh and simulator) with mixed duration tasks. Verify:
  - Head heavy tasks are not delayed (no increase in their start time vs. baseline FIFO).
  - Throughput increases when there’s slack behind HOL.
  - Backfill decisions logged (task id, ERT, slack, reason) for audit.

Risk/Notes
- ERT estimation is conservative by design; default slack margin prevents accidental head delays.

## 2) Capacity‑Aware Placement Filter (optional)

Goal
- Avoid placing/reserving tasks on nodes that can never run them (static capacity < task demand), eliminating “impossible” placements that lead to eternal HOL.

Config (Java, DodoorConf)
- `scheduler.filter.infeasible.nodes.enabled` (default: false)
- `scheduler.filter.infeasible.nodes.strict` (default: true) – if true and no feasible nodes exist, log warn and skip placement for that node in this round; if false, fall back to legacy behavior

Java Changes
- Files:
  - `src/main/java/edu/cam/dodoor/scheduler/taskplacer/CachedTaskPlacer.java`
  - `src/main/java/edu/cam/dodoor/scheduler/taskplacer/RunTimeProbeTaskPlacer.java`
  - `src/main/java/edu/cam/dodoor/scheduler/taskplacer/PrequalTaskPlacer.java`
- Implementation:
  - Before sampling/scoring, build a filtered `nodeAddresses` set:
    - For simulated tasks: compare `taskSpec.resourceRequest` against `resourceCapacityMap[nodeType]`.
    - For real tasks: derive the mapped `TResourceVector` via `_taskNodeStateMap.get(nodeType)` and check against capacity.
  - If filtered set is empty:
    - If `strict=true`, log warn and skip enqueuing this task this round.
    - Else, fall back to unfiltered set (legacy behavior).

Simulator Changes
- Files:
  - `simulator/schedulers/unified_scheduler.py` (TaskPlacer path)
  - `simulator/core/cached_task_placer.py` / `runtime_probe_task_placer.py`
- Mirror the same filtering logic and flags under `config.scheduler.*` to match Java.

Validation
- Create a trace containing tasks exceeding some node types’ capacity. Validate:
  - No reservations on infeasible nodes when enabled.
  - Reduced HOL due to impossible heads.
  - No change in legacy behavior when disabled.

## 3) Simulator Fidelity Improvements (close gaps to Java)

Goal
- Reduce divergence in latency, overhead, and throughput by aligning simulator mechanics with Java services.

Scope & Plan
- Apply mapped resources at node enqueue:
  - File: `simulator/core/simulation_engine.py` in `_handle_task_scheduled`
  - Replace `task.resource_request` with `event.data['task_resources']` when present.
- Late‑binding pre‑lock semantics:
  - File: `simulator/core/simulation_engine.py` `_sparrow_attempt_confirm`
  - Upon confirm attempt, temporarily reserve resources (atomic check+reserve) on the node executor; free on reject.
- Confirm/cancel overhead & message counts:
  - Increment network message counters and add small latency for confirm and cancel paths; parameterize via config (per scheduler overrides).
- Datastore message modeling (Phase 4):
  - File: `simulator/core/cached_simulation_engine.py`, `simulator/core/scheduler_state_cache.py`, `simulator/core/datastore_service.py`
  - Record messages + latency for scheduler→datastore updates and datastore→scheduler broadcasts; keep batch thresholds aligned to Java.
- Cached vs. perfect state consistency:
  - Ensure `UnifiedScheduler` does not mutate `node_states` for cached schedulers in a way that hides staleness; rely on `scheduler_cache` to reflect local deltas and periodic broadcasts.
- Config flags (Simulator)
  - `scheduler.enable_capacity_filter` (default: false)
  - `scheduler.enable_backfill` (default: false)
  - `scheduler.overhead.confirm_ms`, `scheduler.overhead.cancel_ms`, `scheduler.messages.confirm`, `scheduler.messages.cancel`

Validation
- Re‑run `simulator/config/debug_config.json` for all schedulers and compare against `deploy/script/end_to_end_exp/debug.sh` logs:
  - Scheduling latency distributions
  - Total messages and per‑scheduler message ratios
  - Throughput under mixed duration workloads

Rollout
- All features ship disabled by default.
- Document new flags in `deploy/python/scripts/config_generator.py` (for physical runs) and in simulator config schema.
- Add concise logging for backfill decisions, capacity filtering events, and confirm/cancel accounting.
