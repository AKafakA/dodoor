#!/bin/bash

CLOUDLAB_USER="${CLOUDLAB_USER:-$USER}"
# Provide the repository to clone at runtime, avoiding hard-coded owner names.
# Usage: CLOUDLAB_USER=myuser REPO_URL=https://example.com/anon/dodoor.git sh deploy/script/setup.sh
parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  "git clone ${REPO_URL:-<ANON_REPO_URL>}"
echo "Cloning the repository completed."
parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_host  "sudo apt update && sudo apt install -y python3-pip thrift-compiler stress openjdk-17-jdk openjdk-17-jre vim maven stress-ng"
parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_host  "pip install optparse-pretty"
echo "Required packages installed."
parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_host  "cd dodoor && git checkout main && git pull"
echo "Repository cloned and updated to the main branch."
parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_host  "cd dodoor && sh rebuild.sh"
echo "Rebuild completed."
parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_host  "cd dodoor && sh setup_docker.sh"
echo "Docker setup completed."
parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_host  "sudo usermod -a -G docker $CLOUDLAB_USER"
echo "User $CLOUDLAB_USER added to the Docker group."
