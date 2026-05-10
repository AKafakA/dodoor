#!/usr/bin/env bash
#
# Euro-Par 2026 artifact entry point for Dodoor.
#
# Drives the full reproduction pipeline:
#   1) parse the CloudLab manifest into per-host SSH/IP files
#   2) install dependencies + build Dodoor on every node
#   3) run the four experiment campaigns from the paper
#   4) regenerate plots and compare against the reference plots shipped
#      under deploy/plots/
#
# The new run writes to a fresh directory tree so reviewers can compare
# their results against the reference set without overwriting it:
#
#   reference logs : deploy/resources/log/{node,scheduler}/...
#   reference plots: deploy/plots/{azure_600, function_100k_100-0-0, parameter_tune}/
#   new logs       : ${DODOOR_LOG_BASE_DIR}    (default deploy/resources/log_ae)
#   new plots      : ${DODOOR_PLOT_BASE_DIR}   (default deploy/plots_ae)
#   comparison     : ${DODOOR_PLOT_BASE_DIR}/comparison.html
#
# Required env vars:
#   CLOUDLAB_USER  CloudLab username; used for ssh user@... everywhere.
#
# Optional env vars:
#   DODOOR_REPO          (default https://github.com/AKafakA/dodoor.git)
#   DODOOR_BRANCH        (default main)
#   DODOOR_LOG_BASE_DIR  (default deploy/resources/log_ae)
#   DODOOR_PLOT_BASE_DIR (default deploy/plots_ae)
#   QUICK                if set to 1, run reduced workloads (~2h budget)
#
# Usage:
#   ./run.sh                       # full reproduction (default ~8h)
#   QUICK=1 ./run.sh               # reduced reproduction
#   ./run.sh --phase parse         # only re-parse the manifest
#   ./run.sh --phase azure plot    # run azure campaign + (re)plot
#   ./run.sh --phase compare       # only build the comparison report
#
# Phases (any subset, ordered as listed):
#   parse setup azure function tune plot compare
#

set -euo pipefail

cd "$(dirname "$0")"
ROOT="$(pwd)"

# ------------------------------------------------------------------ env

if [[ -z "${CLOUDLAB_USER:-}" ]]; then
  echo "ERROR: CLOUDLAB_USER is unset. Export it (e.g. 'export CLOUDLAB_USER=yourcloudlabid') and re-run." >&2
  exit 2
fi

export CLOUDLAB_USER
export DODOOR_REPO="${DODOOR_REPO:-https://github.com/AKafakA/dodoor.git}"
export DODOOR_BRANCH="${DODOOR_BRANCH:-main}"
export DODOOR_LOG_BASE_DIR="${DODOOR_LOG_BASE_DIR:-deploy/resources/log_ae}"
export DODOOR_PLOT_BASE_DIR="${DODOOR_PLOT_BASE_DIR:-deploy/plots_ae}"
QUICK="${QUICK:-0}"

mkdir -p "${DODOOR_LOG_BASE_DIR}/node" "${DODOOR_LOG_BASE_DIR}/scheduler"
mkdir -p "${DODOOR_PLOT_BASE_DIR}"

# ------------------------------------------------------------------ phases

# Phase order: function_bench runs BEFORE azure. function_bench combos take
# ~10 min each (vs azure's QPS=1 combos which take ~67 min); running fast
# combos first surfaces orchestration bugs quickly instead of 1+ h into the
# slow part of the run.
ALL_PHASES=(parse setup function azure tune plot compare)
if [[ $# -eq 0 ]]; then
  PHASES=("${ALL_PHASES[@]}")
elif [[ "$1" == "--phase" ]]; then
  shift
  PHASES=("$@")
else
  echo "Unrecognized argument: $1" >&2
  echo "Usage: $0 [--phase <name> ...]" >&2
  exit 2
fi

run_phase () { local p="$1"; for x in "${PHASES[@]}"; do [[ "$x" == "$p" ]] && return 0; done; return 1; }

log () { printf '\n=== [run.sh] %s ===\n\n' "$*"; }

# ------------------------------------------------------------------ parse

if run_phase parse; then
  log "Parsing CloudLab manifest into host_addresses/cloud_lab/"
  python3 deploy/python/scripts/cl_manifest_parser.py
  echo "Generated:"
  ls -1 deploy/resources/host_addresses/cloud_lab/

  # ServiceDaemon and TaskTracePlayer on every cluster node read
  # ~/cloud_lab/host_config.json. Push the freshly-parsed file to each
  # host so subsequent campaigns can find it. (cl_manifest_parser has an
  # `upload` flag for this but it defaults to False.)
  log "Distributing host_config.json to all cluster hosts"
  parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host -t 30 \
    "mkdir -p ~/cloud_lab" >/dev/null 2>&1 || true
  parallel-scp -h deploy/resources/host_addresses/cloud_lab/test_host -t 30 \
    deploy/resources/host_addresses/cloud_lab/host_config.json \
    cloud_lab/host_config.json 2>&1 | tail -1
fi

# ------------------------------------------------------------------ setup

if run_phase setup; then
  log "Provisioning all CloudLab nodes (clone + apt install + rebuild + docker)"
  bash deploy/script/setup.sh
fi

# ------------------------------------------------------------------ campaigns

# Quick mode shrinks the QPS sweep, request count, and parameter cardinality
# so the full reproduction fits inside the AE 8h budget while still
# exercising every node and producing every figure. Conclusions remain
# observable; absolute numbers will differ.
#
# Each value below can be overridden via env vars when invoking run.sh, e.g.
#   FUNC_QPS="100 200 300 400" QUICK=1 ./run.sh
# matches the paper's full QPS sweep for the function campaign while leaving
# azure / tune in QUICK form.
#
# Variable names match the env vars the campaign scripts read after the
# env-var-overridable refactor (see deploy/script/end_to_end_exp/*.sh).
if [[ "${QUICK}" == "1" ]]; then
  AZURE_QPS="${AZURE_QPS:-5 20}"
  AZURE_NUM_REQUESTS="${AZURE_NUM_REQUESTS:-1500}"

  FUNC_QPS="${FUNC_QPS:-100 300}"
  FUNC_NUM_REQUESTS="${FUNC_NUM_REQUESTS:-20000}"

  # Tune QUICK defaults match the paper's QPS=100 and overlap a clean
  # 3-of-5 subset of the paper sweep so the resulting plots are directly
  # comparable to deploy/plots/parameter_tune/.
  TUNE_QPS="${TUNE_QPS:-100}"
  TUNE_BATCH_SIZES="${TUNE_BATCH_SIZES:-50 100 150}"
  TUNE_DURATION_WEIGHTS="${TUNE_DURATION_WEIGHTS:-0.0 0.5 1.0}"
  TUNE_NUM_REQUESTS="${TUNE_NUM_REQUESTS:-20000}"
fi

run_campaign () {
  local script="$1"; shift
  log "Running $(basename "$script") $*"
  bash "$script" "$@"
}

if run_phase function; then
  if [[ "${QUICK}" == "1" ]]; then
    QPS="${FUNC_QPS}" NUM_REQUESTS="${FUNC_NUM_REQUESTS}" \
      run_campaign deploy/script/end_to_end_exp/function_bench.sh
  else
    run_campaign deploy/script/end_to_end_exp/function_bench.sh
  fi
fi

if run_phase azure; then
  if [[ "${QUICK}" == "1" ]]; then
    QPS="${AZURE_QPS}" NUM_REQUESTS="${AZURE_NUM_REQUESTS}" \
      run_campaign deploy/script/end_to_end_exp/azure.sh
  else
    run_campaign deploy/script/end_to_end_exp/azure.sh
  fi
fi

if run_phase tune; then
  if [[ "${QUICK}" == "1" ]]; then
    QPS="${TUNE_QPS}" BATCH_SIZES="${TUNE_BATCH_SIZES}" NUM_REQUESTS="${TUNE_NUM_REQUESTS}" \
      run_campaign deploy/script/end_to_end_exp/function_bench_tune_batch_size.sh
    QPS="${TUNE_QPS}" DURATION_WEIGHTS="${TUNE_DURATION_WEIGHTS}" NUM_REQUESTS="${TUNE_NUM_REQUESTS}" \
      run_campaign deploy/script/end_to_end_exp/function_bench_tune_duration_weight.sh
  else
    run_campaign deploy/script/end_to_end_exp/function_bench_tune_batch_size.sh
    run_campaign deploy/script/end_to_end_exp/function_bench_tune_duration_weight.sh
  fi
fi

# ------------------------------------------------------------------ plot

if run_phase plot; then
  log "Regenerating plots from ${DODOOR_LOG_BASE_DIR} → ${DODOOR_PLOT_BASE_DIR}"
  python3 deploy/python/analysis/plot_scheduler.py \
    --log_dir   "${DODOOR_LOG_BASE_DIR}/scheduler" \
    --output_dir "${DODOOR_PLOT_BASE_DIR}"
  python3 deploy/python/analysis/plot_node.py \
    --log_dir   "${DODOOR_LOG_BASE_DIR}/node" \
    --output_dir "${DODOOR_PLOT_BASE_DIR}"
  python3 deploy/python/analysis/plot_parameter_tune.py \
    --log_dir   "${DODOOR_LOG_BASE_DIR}/scheduler" \
    --output_dir "${DODOOR_PLOT_BASE_DIR}/parameter_tune"
fi

# ------------------------------------------------------------------ compare

if run_phase compare; then
  log "Building side-by-side comparison report"
  python3 deploy/python/scripts/compare_results.py \
    --reference deploy/plots \
    --candidate "${DODOOR_PLOT_BASE_DIR}" \
    --output    "${DODOOR_PLOT_BASE_DIR}/comparison.html"
  echo "Open ${DODOOR_PLOT_BASE_DIR}/comparison.html in a browser to review."
fi

log "Done. Phases run: ${PHASES[*]}"
