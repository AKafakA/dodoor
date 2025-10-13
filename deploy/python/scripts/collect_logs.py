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

def collect_glob(host_list, output_dir, remote_glob, local_prefix):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    for host in host_list:
        if host is None:
            continue
        target_name = host.split("@")[1].split(".")[0]
        # Copy and rename on arrival using scp -r with shell expansion
        # We cannot rename per-file easily with wildcards; copy into temp dir then move
        temp_dir = os.path.join(output_dir, f"tmp_{target_name}")
        os.makedirs(temp_dir, exist_ok=True)
        command = f"scp -r {host}:{remote_glob} {temp_dir}/"
        subprocess.call(command, shell=True)
        # Move files with friendly names
        for fname in os.listdir(temp_dir):
            src = os.path.join(temp_dir, fname)
            if os.path.isfile(src):
                dst = os.path.join(output_dir, f"{local_prefix}_{target_name}_{fname}")
                shutil.move(src, dst)
        shutil.rmtree(temp_dir, ignore_errors=True)

scheduler_name = sys.argv[1]
batch_size = sys.argv[2]
beta = sys.argv[3]
cpu_weight = sys.argv[4]
duration_weight = sys.argv[5]
root_dir = sys.argv[6]
qps = sys.argv[7]
debug_logs = len(sys.argv) > 8 and sys.argv[8].lower() == "true"

exp_name = "{}/{}_batch_{}_beta_{}_cpu_{}_duration_{}_qps_{}".format(root_dir,
                                                                      scheduler_name,
                                                                      batch_size,
                                                                      beta,
                                                                      cpu_weight,
                                                                      duration_weight, qps)
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
    collect_logs(node_host, output_dir=target_dir + "/metrics",
                 node_log_name=node_log_name,
                 node_log_target_file_prefix=node_log_target_file_prefix)

    # Collect debug logs if debug flag is enabled
    if debug_logs:
        print("Debug mode enabled: collecting node service output and error logs")
        # Collect node service output logs (.out files)
        node_out_log_name = scheduler_name + "_node_service.out"
        node_out_log_target_file_prefix = scheduler_name + "_node_service_out"
        collect_logs(node_host, output_dir=target_dir + "/service_log",
                     node_log_name=node_out_log_name,
                     node_log_target_file_prefix=node_out_log_target_file_prefix)
        # Collect log4j application logs containing REPLAY lines


with open(scheduler_host_file, "r") as f:
    scheduler_host = []
    scheduler_hosts = f.readlines()
    for host in scheduler_hosts:
        if ip_pattern.match(host) or host_patter.match(host):
            scheduler_host.append(host.strip())

    target_scheduler_dir = "deploy/resources/log/scheduler/{}".format(exp_name)
    scheduler_log_name = scheduler_name + "_scheduler_metrics.log"
    scheduler_log_target_file_prefix = scheduler_name + "_scheduler_metrics"
    collect_logs(scheduler_host, output_dir=target_scheduler_dir + "/metrics",
                 node_log_name=scheduler_log_name,
                 node_log_target_file_prefix=scheduler_log_target_file_prefix)

    # Collect debug logs if debug flag is enabled
    if debug_logs:
        print("Debug mode enabled: collecting scheduler service output and error logs")
        # Collect scheduler service output logs (.out files)
        scheduler_out_log_name = scheduler_name + "_scheduler_service.out"
        scheduler_out_log_target_file_prefix = scheduler_name + "_scheduler_service_out"
        collect_logs(scheduler_host, output_dir=target_scheduler_dir + "/service_log",
                     node_log_name=scheduler_out_log_name,
                     node_log_target_file_prefix=scheduler_out_log_target_file_prefix)
        # Collect scheduler-side application logs (REPLAY lines)
