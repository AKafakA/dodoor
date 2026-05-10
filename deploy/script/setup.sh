#!/bin/bash

# Username on the CloudLab nodes. Defaults to $CLOUDLAB_USER (set by run.sh /
# the AE reviewer) so the scripts run unmodified across accounts.
USER="${CLOUDLAB_USER:-${USER:-asdwb}}"
DODOOR_REPO="${DODOOR_REPO:-https://github.com/AKafakA/dodoor.git}"
DODOOR_BRANCH="${DODOOR_BRANCH:-main}"

parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  "git clone ${DODOOR_REPO}"
echo "Cloning the repository completed."
parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_host  "sudo apt update && sudo apt install -y python3-pip thrift-compiler stress openjdk-17-jdk openjdk-17-jre vim maven stress-ng"
parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_host  "pip install optparse-pretty"
echo "Required packages installed."
parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_host  "cd dodoor && git checkout ${DODOOR_BRANCH} && git pull"
echo "Repository cloned and updated to the main branch."
parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_host  "cd dodoor && sh rebuild.sh"
echo "Rebuild completed."
parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_host  "cd dodoor && sh setup_docker.sh"
echo "Docker setup completed."
parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_host  "sudo usermod -a -G docker $USER"
echo "User $USER added to the Docker group."