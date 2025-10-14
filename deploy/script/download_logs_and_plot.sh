#!/bin/bash

REMOTE_HOSTS="${REMOTE_HOSTS:-user@host.cloudlab.us}"

# Download logs
# Set these paths as needed, avoid hard-coded usernames
LOCAL_REPO_DIR=${LOCAL_REPO_DIR:-$(pwd)}
REMOTE_REPO_DIR=${REMOTE_REPO_DIR:-~/dodoor}

rm -rf "$LOCAL_REPO_DIR/deploy/resources/log/*"
scp -r "$REMOTE_HOSTS:$REMOTE_REPO_DIR/deploy/resources/log/*" "$LOCAL_REPO_DIR/deploy/resources/log/."

# Plot
python3 deploy/python/scripts/plot.py
