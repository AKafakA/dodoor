import re
from deploy.python.analysis.utils import collect_logs


def get_host_address(host_input, test_on_caelum=False):
    caelum_prefix = "wd312@caelum-"
    if test_on_caelum:
        return caelum_prefix + host_input.strip().split(":")[0] + ".cl.cam.ac.uk"
    else:
        return host_input.strip()


node_host = []
caelum_test = True
exp_name = "azure/dodoor"
if caelum_test:
    node_host_file = "deploy/resources/host_addresses/caelum_host_ip"
    scheduler_host_file = "deploy/resources/host_addresses/caelum_scheduler_ip"
    exp_name += "_caelum"
else:
    node_host_file = "deploy/resources/host_addresses/cloud_lab/test_nodes"
    scheduler_host_file = "deploy/resources/host_addresses/cloud_lab/test_scheduler"
    exp_name += "_cloud_lab"

ip_pattern = re.compile("\d+:\d+.\d+.\d+.\d+")
host_patter = re.compile("\w+@\w+")

with open(node_host_file, "r") as f:
    node_hosts = f.readlines()
    for host in node_hosts:
        if ip_pattern.match(host) or host_patter.match(host):
            node_host.append(get_host_address(host, test_on_caelum=caelum_test))

target_dir = "deploy/resources/log/node/{}".format(exp_name)
node_log_name = "dodoor_node_metrics.log"
node_log_target_file_prefix = "dodoor_node_metrics"

scheduler_host = []
with open(scheduler_host_file, "r") as f:
    scheduler_hosts = f.readlines()
    for host in scheduler_hosts:
        if ip_pattern.match(host) or host_patter.match(host):
            scheduler_host.append(get_host_address(host, test_on_caelum=caelum_test))

target_scheduler_dir = "deploy/resources/log/scheduler/{}".format(exp_name)
scheduler_log_name = "dodoor_scheduler_metrics.log"
scheduler_log_target_file_prefix = "dodoor_scheduler_metrics"

collect_logs(node_host, output_dir=target_dir,
             node_log_name=node_log_name,
             node_log_target_file_prefix=node_log_target_file_prefix)

collect_logs(scheduler_host, output_dir=target_scheduler_dir,
             node_log_name=scheduler_log_name,
             node_log_target_file_prefix=scheduler_log_target_file_prefix)
