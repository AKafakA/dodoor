import re

from deploy.python.analysis.utils import collect_logs

node_host = []
node_host_file = "deploy/resources/caelum_host_ip"
host_prefix = "wd312@caelum-"

ip_pattern = re.compile("\d+:\d+.\d+.\d+.\d+")

exp_name = "exp_azure_full_replay/dodoor"
with open(node_host_file, "r") as f:
    node_hosts = f.readlines()
    for host in node_hosts:
        if ip_pattern.match(host):
            node_host.append(host_prefix + host.strip().split(":")[0] + ".cl.cam.ac.uk")

target_dir = "deploy/resources/log/node/{}".format(exp_name)
node_log_name = "dodoor_node_metrics.log"
node_log_target_file_prefix = "dodoor_node_metrics"

collect_logs(node_host, output_dir=target_dir,
             node_log_name=node_log_name,
             node_log_target_file_prefix=node_log_target_file_prefix)


scheduler_host_file = "deploy/resources/caelum_scheduler_ip"
scheduler_host = []
with open(scheduler_host_file, "r") as f:
    scheduler_hosts = f.readlines()
    for host in scheduler_hosts:
        if ip_pattern.match(host):
            scheduler_host.append(host_prefix + host.strip().split(":")[0] + ".cl.cam.ac.uk")

target_scheduler_dir = "deploy/resources/log/scheduler/{}".format(exp_name)
scheduler_log_name = "dodoor_scheduler_metrics.log"
scheduler_log_target_file_prefix = "dodoor_scheduler_metrics"


collect_logs(scheduler_host, output_dir=target_scheduler_dir,
             node_log_name=scheduler_log_name,
             node_log_target_file_prefix=scheduler_log_target_file_prefix)


