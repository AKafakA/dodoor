#!/usr/bin/env bash
#
# Dodoor — Sampled run "headline cells" reproduction (Euro-Par 2026 AE).
#
# Reproduces the two single (campaign, QPS) cells where dodoor's win over
# the canonical Power-of-Two baseline is largest — at FULL paper-exact
# NUM_REQUESTS. This is the headline AE artifact: it fits the 8 h
# Euro-Par AE budget while keeping every measurement at paper-exact
# statistical power.
#
# Position in the testing pyramid:
#   Smoke                 ./smoke.sh                ≤ 2 min   (no cluster)
#   Orchestration check   ./small_cluster_test.sh   ~30 min   (5 nodes)
#   Sampled run           ./headline_cells.sh ←     ~3.5 h    (2 cells × paper-exact reqs)
#   Full run              ./run.sh                  ~39 h     (full sweep × paper-exact reqs)
#
# Headline cells (per paper's reference data) — vs Power-of-Two baseline.
# Power-of-Two is the meaningful, well-known scheduling baseline; random
# is a strawman. Both cells are sized to the QPS where dodoor's saturation
# advantage over Power-of-Two is largest in the paper.
#
#   function @ QPS=400, 100 000 reqs:
#       dodoor p99 = 1117 ms, powerOfTwo p99 = 1457 ms
#       => dodoor improves p99 makespan by 23% over Power-of-Two
#       2 schedulers × ~40 min/combo ≈ 1.3 h wall-clock
#
#   azure   @ QPS=20, 4 000 reqs:
#       dodoor p99 = 2456 ms, powerOfTwo p99 = 3407 ms
#       => dodoor improves p99 makespan by 28% over Power-of-Two
#       2 schedulers × ~84 min/combo ≈ 2.8 h wall-clock
#
# Usage:
#   ./headline_cells.sh function    # function QPS=400 cell only (~1.3 h)
#   ./headline_cells.sh azure       # azure QPS=20 cell only (~2.8 h)
#   ./headline_cells.sh both        # both cells back-to-back (~4.2 h)
#
# The four scheduler dirs land under deploy/resources/log_ae/ in the same
# layout as Tier 3/4, so deploy/python/scripts/compare_results.py can
# regenerate the comparison HTML pointing at this run + the paper.

set -euo pipefail
cd "$(dirname "$0")"

if [[ -z "${CLOUDLAB_USER:-}" ]]; then
  echo "ERROR: CLOUDLAB_USER unset. export CLOUDLAB_USER=<your-cloudlab-id>" >&2
  exit 2
fi

# CRITICAL: route collected logs into the AE log tree, not the shipped
# paper reference. Without this export, collect_logs.py defaults to
# deploy/resources/log/ and shutil.rmtree's the paper data.
export DODOOR_LOG_BASE_DIR="${DODOOR_LOG_BASE_DIR:-deploy/resources/log_ae}"

WHICH="${1:-both}"
case "$WHICH" in
  function|azure|both) ;;
  *) echo "Usage: $0 {function|azure|both}" >&2; exit 2;;
esac

# Make sure the manifest is parsed and host_config.json is on every node.
HA=deploy/resources/host_addresses/cloud_lab
if [[ ! -s "$HA/test_host" ]]; then
  echo "[1/3] Parsing manifest.xml + distributing host_config.json"
  ./run.sh --phase parse
fi

# Cluster cleanup so we don't inherit zombie state.
echo "[2/3] Cluster cleanup"
parallel-ssh -h "$HA/test_host" -t 30 \
  'sudo pkill -9 -f "edu\.cam\.dodoor"; sudo pkill -9 -x stress-ng; rm -f ~/*.log ~/*.out ~/*.err; true' \
  >/dev/null 2>&1 || true

mkdir -p deploy/resources/log_ae/scheduler deploy/resources/log_ae/node deploy/plots_ae

run_function_headline() {
  echo "[*] function bench headline cell: QPS=400, 100 000 reqs, dodoor + powerOfTwo"
  local START=$(date +%s)
  SCHEDULERS="powerOfTwo dodoor" \
    QPS="400" \
    NUM_REQUESTS=100000 \
    EXPERIMENT_TIMEOUT_IN_MIN=40 \
    DEBUG_LOGS=false \
    bash deploy/script/end_to_end_exp/function_bench.sh
  echo "[*] function headline cell wall-clock: $(( ($(date +%s) - START) / 60 )) min"
}

run_azure_headline() {
  echo "[*] azure headline cell: QPS=20, 4 000 reqs, dodoor + powerOfTwo"
  local START=$(date +%s)
  SCHEDULERS="powerOfTwo dodoor" \
    QPS="20" \
    NUM_REQUESTS=4000 \
    EXPERIMENT_TIMEOUT_IN_MIN=600 \
    DEBUG_LOGS=false \
    bash deploy/script/end_to_end_exp/azure.sh
  echo "[*] azure headline cell wall-clock: $(( ($(date +%s) - START) / 60 )) min"
}

echo "[3/3] Running headline cell(s): $WHICH"
case "$WHICH" in
  function) run_function_headline ;;
  azure)    run_azure_headline ;;
  both)     run_function_headline; run_azure_headline ;;
esac

# Plot + comparison HTML
echo "[*] Regenerating plots and comparison report"
python3 deploy/python/analysis/plot_scheduler.py \
  --log_dir deploy/resources/log_ae/scheduler --output_dir deploy/plots_ae 2>&1 | tail -3 || true
python3 deploy/python/analysis/plot_node.py \
  --log_dir deploy/resources/log_ae/node --output_dir deploy/plots_ae 2>&1 | tail -3 || true
python3 deploy/python/scripts/compare_results.py \
  --reference deploy/plots --candidate deploy/plots_ae \
  --output deploy/plots_ae/comparison.html 2>&1 | tail -3 || true

echo
echo "Headline-cell reproduction complete. Open:"
echo "  deploy/plots_ae/comparison.html"
echo "Verify dodoor p99 makespan vs paper for the chosen cell(s):"
echo "  function@QPS=400: paper dodoor=1117ms, paper powerOfTwo=1457ms (dodoor -23%)"
echo "  azure@QPS=20:     paper dodoor=2456ms, paper powerOfTwo=3407ms (dodoor -28%)"
