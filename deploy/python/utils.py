import os


def collect_logs(host_list,
                 output_dir,
                 node_log_name,
                 node_log_target_file_prefix,
                 node_log_target_file_suffix=".log"):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    for host in host_list:
        target_name = host.split("@")[1].split(".")[0].split("-")[1]
        target_file_path = os.path.join(os.getcwd(),
                                        output_dir + "/" + node_log_target_file_prefix + "_"
                                        + target_name + node_log_target_file_suffix)
        os.system("scp -r %s:%s %s" % (host, node_log_name, os.path.join(os.getcwd(), target_file_path)))
        print("{} logs collected.".format(host))
    return


if __name__ == "__main__":
    test_host_list = ["wd312@caelum-103", "wd312@caelum-104"]
    collect_logs(test_host_list, output_dir="deploy/resources/log/node/test",
                 node_log_name="dodoor_node_metrics.log",
                 node_log_target_file_prefix="dodoor_node_metrics")

    test_scheduler_lost_host_list = ["wd312@caelum-102"]
    collect_logs(test_scheduler_lost_host_list, output_dir="deploy/resources/log/scheduler/test",
                 node_log_name="dodoor_scheduler_metrics.log",
                 node_log_target_file_prefix="dodoor_scheduler_metrics")

    test_data_store_host_list = ["wd312@caelum-102"]
    collect_logs(test_data_store_host_list, output_dir="deploy/resources/log/datastore/test",
                 node_log_name="dodoor_datastore_metrics.log",
                 node_log_target_file_prefix="dodoor_datastore_metrics")
