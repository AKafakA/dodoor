#!/bin/bash
#
# Pull experiment logs from the control host (where parallel-ssh + log
# collection ran) into the local checkout, then regenerate plots.
#
# Override the defaults via env vars or skip this script entirely if you ran
# everything locally on the control host:
#   REMOTE_HOSTS    : user@host of the control machine (e.g. asdwb@caelum-103)
#   REMOTE_REPO_DIR : path to the dodoor checkout on that machine
#   LOCAL_REPO_DIR  : path to the dodoor checkout on this machine

REMOTE_HOSTS="${REMOTE_HOSTS:-${USER}@localhost}"
REMOTE_REPO_DIR="${REMOTE_REPO_DIR:-~/Code/scheduling/dodoor}"
LOCAL_REPO_DIR="${LOCAL_REPO_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"

# Download logs
rm -rf "${LOCAL_REPO_DIR}/deploy/resources/log/"*
scp -r "${REMOTE_HOSTS}:${REMOTE_REPO_DIR}/deploy/resources/log/"* "${LOCAL_REPO_DIR}/deploy/resources/log/."

# Plot
cd "${LOCAL_REPO_DIR}" && python3 deploy/python/analysis/plot_scheduler.py
cd "${LOCAL_REPO_DIR}" && python3 deploy/python/analysis/plot_node.py
