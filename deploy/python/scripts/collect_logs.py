import re
import sys
import os
import shutil
import subprocess


def collect_logs(host_list,
                 output_dir,
                 node_log_name,
                 node_log_target_file_prefix,
                 node_log_target_file_suffix=".log"):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    else:
        shutil.rmtree(output_dir, ignore_errors=True)
        os.makedirs(output_dir)
    for host in host_list:
        if host == None:
            continue
        target_name = host.split("@")[1].split(".")[0]
        target_file_path = os.path.join(os.getcwd(),
                                        output_dir + "/" + node_log_target_file_prefix + "_"
                                        + target_name + node_log_target_file_suffix)
        command = "scp -r %s:%s %s" % (host, node_log_name, target_file_path)
        subprocess.call(command, shell=True)
        print("{} logs collected.".format(host))
    return

scheduler_name = sys.argv[1]
batch_size = sys.argv[2]
beta = sys.argv[3]
cpu_weight = sys.argv[4]
duration_weight = sys.argv[5]
system_load = sys.argv[6]
root_dir = sys.argv[7]
qps = sys.argv[8]

exp_name = "{}/{}_batch_{}_beta_{}_cpu_{}_duration_{}_load_{}_qps_{}".format(root_dir,
                                                                      scheduler_name,
                                                                      batch_size,
                                                                      beta,
                                                                      cpu_weight,
                                                                      duration_weight,
                                                                      system_load, qps)
node_host = []
node_host_file = "deploy/resources/host_addresses/cloud_lab/test_nodes"
scheduler_host_file = "deploy/resources/host_addresses/cloud_lab/test_scheduler"


ip_pattern = re.compile("\d+:\d+.\d+.\d+.\d+")
host_patter = re.compile("\w+@\w+")

with open(node_host_file, "r") as f:
    node_hosts = f.readlines()
    for host in node_hosts:
        if ip_pattern.match(host) or host_patter.match(host):
            node_host.append(host.strip())

    target_dir = "deploy/resources/log/node/{}".format(exp_name)
    node_log_name = scheduler_name + "_node_metrics.log"
    node_log_target_file_prefix = scheduler_name + "_node_metrics"
    collect_logs(node_host, output_dir=target_dir,
                 node_log_name=node_log_name,
                 node_log_target_file_prefix=node_log_target_file_prefix)


with open(scheduler_host_file, "r") as f:
    scheduler_host = []
    scheduler_hosts = f.readlines()
    for host in scheduler_hosts:
        if ip_pattern.match(host) or host_patter.match(host):
            scheduler_host.append(host.strip())

    target_scheduler_dir = "deploy/resources/log/scheduler/{}".format(exp_name)
    scheduler_log_name = scheduler_name + "_scheduler_metrics.log"
    scheduler_log_target_file_prefix = scheduler_name + "_scheduler_metrics"
    collect_logs(scheduler_host, output_dir=target_scheduler_dir,
                 node_log_name=scheduler_log_name,
                 node_log_target_file_prefix=scheduler_log_target_file_prefix)
