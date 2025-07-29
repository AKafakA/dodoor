#!/bin/bash

USER="asdwb"
parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  "git clone https://github.com/AKafakA/dodoor.git"
echo "Cloning the repository completed."
parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_host  "sudo apt update && sudo apt install -y python3-pip thrift-compiler stress openjdk-17-jdk openjdk-17-jre vim maven stress-ng"
parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_host  "pip install optparse-pretty"
echo "Required packages installed."
parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_host  "cd dodoor && git checkout main && git pull && sh rebuild.sh"
echo "Repository cloned and updated to the main branch with maven project built."
parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_host  "cd dodoor && sh setup_docker.sh"
echo "Docker setup completed."
parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_host  "sudo usermod -a -G docker $USER"
echo "User $USER added to the Docker group."