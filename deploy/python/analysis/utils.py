import os
import subprocess
import shutil


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
