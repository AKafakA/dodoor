parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  "sudo rm -rf dodoor"
parallel-ssh -h deploy/resources/host_addresses/cloud_lab/test_host  "git clone https://github.com/AKafakA/dodoor.git"
parallel-ssh -t 0 -h deploy/resources/host_addresses/cloud_lab/test_host  "cd dodoor && git checkout main && git pull && sh rebuild.sh && sh setup_docker.sh"