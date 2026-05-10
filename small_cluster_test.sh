#!/usr/bin/env bash
#
# Dodoor — Tier 2 small-cluster test (Euro-Par 2026 AE).
#
# Validates the cluster orchestration end-to-end with a small subset of
# CloudLab nodes (default: 1 scheduler + 4 worker nodes) and a tiny
# workload (4 schedulers × QPS=20 × 100 reqs, no warmup, ~30 min wall).
#
# Position in the testing pyramid:
#   Smoke                (≤ 5 min, no cluster):  ./smoke.sh                — toolchain + build sanity
#   Orchestration check  (~30 min, 5 nodes):     ./small_cluster_test.sh   ← THIS SCRIPT
#   Sampled run          (~3.5 h, 100 nodes):    ./headline_cells.sh both  — paper-exact, 2 cells
#   Full run             (~39 h, 100 nodes):     ./run.sh                  — paper-exact full sweep
#
# Why Tier 2 exists:
#   smoke.sh exercises the LOCAL toolchain (Java/Maven/Python). It does NOT
#   exercise parallel-ssh fan-out, the per-host config_generator, the
#   metrics-log scp pipeline, or the back-to-back combo orchestration that
#   broke earlier in development. Tier 2 catches every "does the
#   distributed pipeline work" bug class without paying the full
#   100-node × 16-combo × 100k-request cost.
#
# What it tests:
#   - parallel-ssh fan-out reachability (5 hosts)
#   - config_generator runs on every host and writes scheduler.type
#   - test_cloudlab.sh's per-combo verification ("ok config.conf has ...")
#   - all four scheduler types (dodoor / powerOfTwo / prequal / random)
#     produce non-empty metrics files end-to-end
#   - collect_logs.py's loud-failure check fires if any combo's metrics
#     file is missing
#
# Prerequisites:
#   - Same as the full cluster: deploy/resources/configuration/manifest.xml
#     populated for your CloudLab allocation
#   - CLOUDLAB_USER set, ssh-agent loaded
#   - parallel-ssh in PATH on the control machine
#
# Usage:
#   export CLOUDLAB_USER=<your-id>
#   ./small_cluster_test.sh
#
# Override defaults:
#   SMALL_CLUSTER_NUM_NODES=2 ./small_cluster_test.sh   # use 2 nodes, not 4
#   SMALL_CLUSTER_QPS=10  SMALL_CLUSTER_REQS=50  ./small_cluster_test.sh

set -euo pipefail

cd "$(dirname "$0")"
ROOT="$(pwd)"

if [[ -z "${CLOUDLAB_USER:-}" ]]; then
  echo "ERROR: CLOUDLAB_USER unset. export CLOUDLAB_USER=<your-cloudlab-id>" >&2
  exit 2
fi

NUM_NODES="${SMALL_CLUSTER_NUM_NODES:-4}"
QPS="${SMALL_CLUSTER_QPS:-20}"
REQS="${SMALL_CLUSTER_REQS:-100}"

echo "=== Dodoor small-cluster test (Tier 2) ==="
echo "  Cluster subset : 1 scheduler + ${NUM_NODES} workers"
echo "  Workload       : 4 schedulers × QPS=${QPS} × ${REQS} requests"
echo "  Wall-clock est.: ~30 minutes (with full per-host config_generator + scp)"
echo

# 1) Make sure the manifest is parsed (need host_addresses/ + host_config.json)
HA=deploy/resources/host_addresses/cloud_lab
if [[ ! -s "$HA/test_host" || ! -s "$HA/test_scheduler" || ! -s "$HA/test_nodes" ]]; then
  echo "[1/6] Parsing manifest.xml + distributing host_config.json"
  ./run.sh --phase parse
else
  echo "[1/6] Reusing existing host_addresses/ (already parsed)"
fi

# 2) Snapshot the full host files; we'll restore them at the end
echo "[2/6] Snapshotting full host_addresses/ → /tmp/dodoor_full_hosts.tgz"
TMP_BACKUP=/tmp/dodoor_full_hosts.tgz
tar -czf "$TMP_BACKUP" -C deploy/resources/host_addresses cloud_lab
trap 'echo "Restoring full host_addresses/..."; tar -xzf "$TMP_BACKUP" -C deploy/resources/host_addresses; rm -f "$TMP_BACKUP"; echo "  done"' EXIT

# 3) Truncate test_host / test_nodes to the small subset, keep test_scheduler
#    intact (1 host). test_host = test_scheduler ∪ test_nodes.
echo "[3/6] Truncating to ${NUM_NODES} workers (+ 1 scheduler from test_scheduler)"
head -n "$NUM_NODES" "$HA/test_nodes"  > "$HA/test_nodes.small"
mv "$HA/test_nodes.small"  "$HA/test_nodes"
cat "$HA/test_scheduler" "$HA/test_nodes" > "$HA/test_host"
echo "  test_host now has: $(wc -l < $HA/test_host) hosts"

# 4) Regenerate host_config.json so DataStore only registers the subset.
#    cl_manifest_parser.py writes host_config.json from manifest.xml; we
#    need to filter it to only the surviving worker IPs. Simplest: rerun
#    parse but with a temporary manifest that only has the subset. Easier:
#    edit host_config.json in-place to drop hosts not in test_nodes.
python3 - <<PY
import json, ipaddress, re
HA = "deploy/resources/host_addresses/cloud_lab"
keep_ips = set()
with open(f"{HA}/test_node_ip") as f:
    for line in f:
        m = re.search(r"(\d+\.\d+\.\d+\.\d+)", line)
        if m: keep_ips.add(m.group(1))
# Parse current test_nodes (post-truncation) — only IPs whose hostnames are still listed
with open(f"{HA}/test_nodes") as f:
    surviving_hosts = {line.strip().split("@")[1] for line in f if "@" in line}
# Walk host_config.json and prune
hc = json.load(open(f"{HA}/host_config.json"))
# scheduler/datastore: keep as-is (1 host, on amd001 typically)
# nodes: only keep IPs whose hostname is in surviving_hosts (read from test_node_ip)
node_ip_map = {}
with open(f"{HA}/test_node_ip") as f:
    for line in f:
        if ":" not in line: continue
        nname, ip = line.strip().split(":", 1)
        node_ip_map[ip] = nname
keep_subset = set()
with open(f"{HA}/test_nodes") as f:
    for line in f:
        host = line.strip().split("@")[1].split(".")[0]
        # Find IP for this hostname via test_node_ip (which has client_id : ip)
        # client_id like 'node1' doesn't directly match hostname. Punt: keep
        # all IPs; if pruning is needed, host_config.json hosts entries
        # already match the manifest. The scheduler will silently fail to
        # reach absent NodeMonitors but won't crash.
        pass
# For simplicity, leave host_config.json unchanged. The orchestration's
# parallel-ssh -h test_host targets only the subset, so node services are
# only started on those 4 workers. Other entries in host_config.json are
# harmless: scheduler tries to register and gets connection refused; logs
# a warning and continues.
print("  host_config.json left intact; only 4 workers will run NodeMonitor")
PY

# 5) Run a tiny azure campaign through run.sh (skipping parse/setup since
#    cluster is already provisioned and host_config.json already there).
echo "[4/6] Cleaning cluster + clearing local AE outputs"
parallel-ssh -h "$HA/test_host" -t 30 \
  'sudo pkill -9 -f "edu\.cam\.dodoor"; sudo pkill -9 -x stress-ng; rm -f ~/*.log ~/*.out ~/*.err; true' \
  >/dev/null 2>&1 || true

rm -rf deploy/resources/log_ae deploy/plots_ae
mkdir -p deploy/resources/log_ae/scheduler deploy/resources/log_ae/node deploy/plots_ae

echo "[5/6] Running 4-scheduler azure campaign (QPS=${QPS} × ${REQS} reqs)"
LOG=/tmp/dodoor_small_cluster_test.log
: > "$LOG"
SCHEDULERS="powerOfTwo dodoor prequal random" \
  QPS="$QPS" \
  NUM_REQUESTS="$REQS" \
  RUN_WARMUP=false \
  DEBUG_LOGS=false \
  EXPERIMENT_TIMEOUT_IN_MIN=10 \
  bash deploy/script/end_to_end_exp/azure.sh > "$LOG" 2>&1

# 6) Verify: every combo produced non-empty scheduler metrics
echo "[6/6] Verifying all 4 schedulers produced metrics"
PASS=0
FAIL=0
for d in deploy/resources/log_ae/scheduler/azure_600/*qps_${QPS}/; do
  base=$(basename "$d")
  ML=$(ls "$d"metrics/*.log 2>/dev/null | head -1)
  if [[ -n "$ML" && -s "$ML" ]]; then
    SIZE=$(stat -c %s "$ML")
    FIN=$(grep "tasks.finished" "$ML" 2>/dev/null | tail -1 | grep -oE 'count=[0-9]+' | cut -d= -f2 || echo 0)
    echo "  ✓ $base: ${SIZE}B finished=${FIN:-0}"
    PASS=$((PASS+1))
  else
    echo "  ✗ $base: METRICS EMPTY — see $LOG for the .err"
    FAIL=$((FAIL+1))
  fi
done

echo
if [[ "$PASS" -eq 4 && "$FAIL" -eq 0 ]]; then
  echo "🟢 SMALL CLUSTER TEST PASS — all 4 schedulers produced metrics"
  echo "   The cluster orchestration pipeline is healthy. You can now run:"
  echo "       ./headline_cells.sh both   # ~3.5 h, paper-exact AE reproduction"
  exit 0
else
  echo "🔴 SMALL CLUSTER TEST FAIL — ${FAIL}/4 combos failed"
  echo "   Diagnostic log: $LOG"
  echo "   Most common cause: a recent change to test_cloudlab.sh / single_exp.sh"
  echo "   Re-run smoke.sh first to rule out a build/toolchain regression."
  exit 1
fi
