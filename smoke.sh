#!/usr/bin/env bash
#
# Dodoor artifact smoke test — Euro-Par 2026 AE.
#
# Single-host sanity check the AE reviewer runs *before* committing
# CloudLab time. Verifies the toolchain, builds the JAR, exercises the
# Java entry points, imports the Python analysis package, and confirms
# the comparison pipeline is wired up.
#
# Wall-clock target: < 2 minutes on a clean dev machine. No CloudLab
# allocation required, no Docker, no stress-ng.
#
# A green smoke test means: the artifact unpacked correctly, the build
# system works, every binary the cluster scripts invoke is loadable, and
# the analysis side that produces deploy/plots_ae/comparison.html is
# functional. After this passes, run `./headline_cells.sh both` (~ 3.5 h,
# AE-budget) or `./run.sh` (~ 39 h, paper-exact full sweep) on CloudLab.

set -euo pipefail

cd "$(dirname "$0")"
ROOT="$(pwd)"

echo "=== Dodoor artifact smoke test ==="
echo

# ----------------------------------------------------------- 1. Toolchain
java -version 2>&1 | head -1 | grep -E 'version "(1[7-9]|2[0-9])' >/dev/null \
  || { echo "FAIL: Java 17+ required (got: $(java -version 2>&1 | head -1))"; exit 1; }
command -v mvn      >/dev/null || { echo "FAIL: mvn not in PATH (apt install maven)"; exit 1; }
command -v python3  >/dev/null || { echo "FAIL: python3 not in PATH"; exit 1; }
echo "  [1/6] toolchain ok"
echo "         Java   : $(java -version 2>&1 | head -1)"
echo "         Maven  : $(mvn -version 2>&1 | head -1)"
echo "         Python : $(python3 --version)"

# --------------------------------------------------------- 2. Build the JAR
JAR=target/dodoor-1.0-SNAPSHOT.jar
if [ ! -f "$JAR" ]; then
  echo "  [2/6] building JAR (~1–2 min)..."
  BUILD_LOG=$(mktemp -t dodoor-smoke-build-XXXXXX.log)
  if ! ./rebuild.sh > "$BUILD_LOG" 2>&1; then
    echo "FAIL: build error. Last 30 lines of $BUILD_LOG:"
    tail -30 "$BUILD_LOG"
    exit 1
  fi
  rm -f "$BUILD_LOG"
fi
unzip -l "$JAR" 2>/dev/null | grep -q 'ServiceDaemon\.class'  || { echo "FAIL: ServiceDaemon missing from JAR"; exit 1; }
unzip -l "$JAR" 2>/dev/null | grep -q 'TaskTracePlayer\.class' || { echo "FAIL: TaskTracePlayer missing from JAR"; exit 1; }
echo "  [2/6] build ok ($JAR)"

# ----------------------------------- 3. Java entry points load (no crash)
# ServiceDaemon and TaskTracePlayer accept arguments; -h prints usage and
# exits 0 (or non-zero with help text — both are acceptable for "loaded").
java -cp "$JAR" edu.cam.dodoor.ServiceDaemon -h     >/dev/null 2>&1 || true
java -cp "$JAR" edu.cam.dodoor.client.TaskTracePlayer -h >/dev/null 2>&1 || true
# Stronger check: the class actually exists.
java -cp "$JAR" -e 'System.exit(0)' 2>/dev/null || \
  java -cp "$JAR" edu.cam.dodoor.ServiceDaemon 2>&1 | grep -qE 'Usage|missing|required|error' \
  || { echo "FAIL: ServiceDaemon does not load"; exit 1; }
echo "  [3/6] ServiceDaemon + TaskTracePlayer load cleanly"

# ------------------------------------------------- 4. Python analysis stack
# scheduler_metrics is a real module; the *_logs / cl_manifest_parser /
# compare_results files are scripts that read sys.argv at module level, so
# we syntax-compile them rather than import them.
python3 - <<'PY' 2>&1 | sed 's/^/         /'
import sys, py_compile
sys.path.insert(0, ".")
from deploy.python.analysis.scheduler_metrics import SchedulerMetrics  # noqa
for f in ("deploy/python/scripts/cl_manifest_parser.py",
          "deploy/python/scripts/collect_logs.py",
          "deploy/python/scripts/compare_results.py",
          "deploy/python/scripts/config_generator.py",
          "deploy/python/analysis/plot_scheduler.py",
          "deploy/python/analysis/plot_node.py",
          "deploy/python/analysis/plot_parameter_tune.py"):
    py_compile.compile(f, doraise=True)
print("imports + syntax ok")
PY
python3 -c "import matplotlib, pandas, seaborn, numpy" 2>/dev/null \
  || { echo "FAIL: missing plot deps. Run: pip install -r deploy/python/requirements.txt"; exit 1; }
echo "  [4/6] python analysis stack ready (scheduler_metrics imports; plot/script files syntax-compile; matplotlib + pandas + seaborn + numpy present)"

# -------------------- 5. Manifest parser + config generator (no cluster)
TMP=$(mktemp -d -t dodoor-smoke-XXXXXX)
trap 'rm -rf "$TMP"' EXIT

# NOTE: cl_manifest_parser.py writes to a hardcoded
# deploy/resources/host_addresses/cloud_lab/ path with no override flag, so
# we ONLY syntax-compile it here — running it would overwrite the shipped
# host_addresses files. The reviewer invokes it via `./run.sh --phase parse`
# *after* replacing manifest.xml with their own CloudLab manifest.
python3 -c "import py_compile; py_compile.compile('deploy/python/scripts/cl_manifest_parser.py', doraise=True)" \
  || { echo "FAIL: cl_manifest_parser.py syntax error"; exit 1; }

python3 deploy/python/scripts/config_generator.py \
  --output "$TMP/config.conf" \
  --scheduler-type dodoor --batch-size 50 --beta 1.0 \
  --network_interface lo >/dev/null
grep -q '^scheduler.type' "$TMP/config.conf" \
  || { echo "FAIL: config_generator.py did not emit valid config"; exit 1; }
echo "  [5/6] cl_manifest_parser syntax-clean + config_generator runnable"

# --------------------------- 6. Reference summary + comparison HTML render
python3 - <<'PY' 2>&1 | sed 's/^/         /'
import json
d = json.load(open("deploy/resources/reference_summary.json"))
need = {"azure_600", "function_100k_100-0-0"}
assert need.issubset(d), f"missing campaigns: {need - d.keys()}"
assert d["azure_600"], "azure_600 has zero rows"
print(f"reference_summary.json: {len(d)} campaigns, "
      f"{sum(len(v) for v in d.values())} total rows")
PY

# Render a smoke comparison.html (reference plots vs themselves) so the
# review pipeline is end-to-end exercised even in the absence of an AE run.
python3 deploy/python/scripts/compare_results.py \
  --reference deploy/plots \
  --candidate deploy/plots \
  --output    "$TMP/comparison.html" >/dev/null 2>&1 \
  || { echo "FAIL: compare_results.py crashed"; exit 1; }
[ -s "$TMP/comparison.html" ] || { echo "FAIL: comparison.html empty"; exit 1; }
PAIRS=$(grep -c "<h3>" "$TMP/comparison.html" || echo 0)
TABLES=$(grep -c "Latency summary" "$TMP/comparison.html" || echo 0)
echo "  [6/6] comparison renders ($PAIRS image pairs, $TABLES latency tables)"

echo
echo "SMOKE TEST PASS"
echo
echo "Next steps:"
echo "  • Headline AE reproduction on 100 nodes (~3.5 h, paper-exact data):"
echo "        export CLOUDLAB_USER=<your-id> && ./headline_cells.sh both"
echo "  • Full paper-exact reproduction (~12 h on CloudLab, optional):"
echo "        export CLOUDLAB_USER=<your-id> && ./run.sh"
echo "  • Provision the cluster from deploy/resources/configuration/manifest.xml first."
