from optparse import OptionParser


def parse_args():
    parser = OptionParser(usage="dodoor-config [options]" +
                                "\n\n generate the configuration file for dodoor experiments")
    parser.add_option("--scheduler-type", default="dodoor",
                      help="The type of scheduler to be used in the experiment")
    parser.add_option("--use-configable-address", default=False,
                      help="Whether to use the configable address for the services")
    parser.add_option("-n", "--nodes-file",
                      help="Inject the host ip addresses of nodes into the config file from the host files")
    parser.add_option("-s", "--scheduler-file",
                      help="Inject the scheduler ip address into the config file from the scheduler file")
    parser.add_option("-d", "--data-store-file",
                      help="Inject the data store ip address into the config file from the data store file")
    parser.add_option("--num-schedulers", default=1,
                      help="The number of schedulers in the system")
    parser.add_option("--num-nodes", default=100,
                      help="The number of nodes in the system")
    parser.add_option("--num-data-stores", default=1,
                      help="The number of data stores in the system")
    parser.add_option("--scheduler-data-store-colocated", default=True,
                      help="Whether the scheduler and data store are colocated on the same host")
    parser.add_option("-o", "--output", default="./config.conf",
                      help="The output path of generated configuration file")
    parser.add_option("--node-monitor-ports", default="20501",
                      help="The port numbers of the node, passing multiple options separated by comma. Different ports "
                           "will point to a singleton node monitor instances")
    parser.add_option("--node-enqueue-ports", default="20502",
                      help="The port number of the internal service to enqueue and dequeue the tasks")
    parser.add_option("--data-store-ports", default="20503",
                      help="The port number of the data store, passing multiple options separated by comma, "
                           "same as scheduler ports to create multiple data stores instances")
    parser.add_option("--scheduler-ports", default="20504",
                      help="The port numbers of the scheduler, passing multiple options separated by comma. "
                           "Each port will be used by a individual scheduler. So, the number of ports should be equal "
                           "to the number of schedulers per host.")
    parser.add_option("--scheduler-thrift-threads", default=8,
                      help="The number of threads running in scheduler service to listen to the thrift requests")
    parser.add_option("--node-monitor-thrift-threads", default=1,
                      help="The number of threads running in node monitor to listen to the thrift requests")
    parser.add_option("--data-store-thrift-threads", default=8,
                      help="The number of threads running in data store to listen to the thrift requests")
    parser.add_option("--node-enqueue-thrift-threads", default=1,
                      help="The number of threads running in internal service to listen to the thrift requests")
    parser.add_option("-t", "--trace-enabled",
                      default=True, help="whether to enable the trace of the system status")
    parser.add_option("--node_trace-file", default="node_metrics.log",
                      help="The trace file of node service to be used in the experiment")
    parser.add_option("--datastore_trace-file", default="datastore_metrics.log",
                      help="The trace file of datastore service to be used in the experiment")
    parser.add_option("--scheduler_trace-file", default="scheduler_metrics.log",
                      help="The trace file of scheduler service to be used in the experiment")
    parser.add_option("--tracking-interval", default=10,
                      help="The interval in seconds of tracking the system status")
    parser.add_option("--cores", default=6,
                      help="The number of available cores in the system to run the tasks")
    parser.add_option("--memory", default=30720,
                      help="The amount of memory in Mb in the system to run the tasks")
    parser.add_option("--disk", default=307200,
                      help="The amount of disk in Mb in the system to run the tasks")
    parser.add_option("--num-slots", default=4,
                      help="The number of slots in each node to run the tasks in parallel. So, "
                           "the maximal resources can be used by the tasks like cores will be cores / num_slots."
                           "And the large tasks exceeding the resources will be rejected.")
    parser.add_option("--beta", default=0.85,
                      help="The beta value used for 1 + beta process for random scheduling")
    parser.add_option("--batch-size", default=82,
                      help="The batch size of the tasks to be scheduled by dodoor scheduler")
    parser.add_option("--scheduler-num-tasks-update", default=8,
                      help="The number of tasks to be scheduled before scheduler update its load views in datastore")
    parser.add_option("--replay_with_delay", default=True,
                      help="Whether to replay the trace following the provided timeline and "
                           "delay the tasks kicking off until the time comes.")
    parser.add_option("--replay_with_disk", default=False,
                      help="Whether to replay the trace with disks requirements or just ignore it.")
    parser.add_option("--num_thread_concurrent_submitted_tasks", default=1000,
                      help="The number of threads concurrently submitting the tasks to scheduler from trace player.")
    parser.add_option("--network_interface", default="eno1",
                      help="The network interface to be used for the network communication")
    parser.add_option("--cpu_weight", default="1.0",
                      help="The weight of cpu in the dodoor load score calculation.")
    parser.add_option("--memory_weight", default="1.0",
                      help="The weight of memory in the dodoor load score calculation.")
    parser.add_option("--disk_weight", default="1.0",
                      help="The weight of disk in the dodoor load score calculation.")
    parser.add_option("--duration_weight", default="0.95",
                      help="The weight of total pending task duration in the dodoor load score calculation.")

    # generate configuration for prequal
    parser.add_option("--prequal_probe_ratio", default=3,
                      help="The ratio of probe tasks to the total tasks in the system")
    parser.add_option("--prequal_probe_pool_size", default=16,
                        help="The pool size of probe tasks to be scheduled")
    parser.add_option("--prequal_rif_quantile", default=0.84,
                        help="The quantile of RIF to be used in the prequal scheduler")
    parser.add_option("--prequal_probe_reuse_budget", default=2,
                        help="The reuse budget of probe tasks to be scheduled")
    parser.add_option("--prequal_probe_remove_interval_ms", default=1000,
                        help="The interval in milliseconds to remove the probe tasks from the system")

    parser.add_option("--task_replay_time_scale", default=1.0,
                      help="The time scale to replay the tasks in the trace, used to speed up the replay process for debugging scheduler performances")
    return parser.parse_args()


# adding options
def main():
    # add options
    (options, args) = parse_args()
    print("Generating configuration file at " + options.output)

    config_path = options.output
    file = open(config_path, "w")

    if options.use_configable_address:
        with open(options.nodes_file, "r") as f:
            write_ip_port(file, f.readlines(), "static.node")

        with open(options.scheduler_file, "r") as f:
            write_ip_port(file, f.readlines(), "static.scheduler")

        with open(options.data_store_file, "r") as f:
            write_ip_port(file, f.readlines(), "static.datastore")
    else:
        num_scheduler = int(options.num_schedulers)
        num_nodes = int(options.num_nodes)
        num_data_stores = int(options.num_data_stores)
        scheduler_data_store_colocated = options.scheduler_data_store_colocated
        start_ip_prefix = "10.10.1."
        j = 0
        if scheduler_data_store_colocated:
            assert num_scheduler == num_data_stores, "The number of schedulers and data stores should be the same"
            address = ""
            for i in range(num_scheduler):
                address += start_ip_prefix + str(j + 1) + ","
                j += 1
            address = address[:-1]
            file.write("static.scheduler = " + address + "\n")
            file.write("static.datastore = " + address + "\n")
        else:
            address = ""
            for i in range(num_scheduler):
                address += start_ip_prefix + str(j + 1) + ","
                j += 1
            address = address[:-1]
            file.write("static.scheduler = " + address + "\n")

            address = ""
            for i in range(j, j + num_data_stores):
                address += start_ip_prefix + str(j + 1) + ","
                j += 1
            address = address[:-1]
            file.write("static.datastore = " + address + "\n")

        address = ""
        for i in range(j, j + num_nodes):
            address += start_ip_prefix + str(j + 1) + ","
            j += 1
        address = address[:-1]
        file.write("static.node = " + address + "\n")

    scheduler_type = options.scheduler_type
    file.write("scheduler.type = {} \n".format(scheduler_type))

    if options.trace_enabled:
        file.write("tracking.enabled = true \n")
        file.write("tracking.interval.seconds = {} \n".format(options.tracking_interval))
        file.write("node.metrics.log.file = {} \n".format(options.node_trace_file))
        file.write("datastore.metrics.log.file = {} \n".format(options.datastore_trace_file))
        file.write("scheduler.metrics.log.file = {} \n".format(options.scheduler_trace_file))

    file.write("system.cores = {} \n".format(options.cores))

    file.write("system.memory = {} \n".format(options.memory))
    file.write("system.disk = {} \n".format(options.disk))
    file.write("node_monitor.num_slots = {} \n".format(options.num_slots))

    file.write("dodoor.beta = {} \n".format(options.beta))
    file.write("dodoor.batch_size = {} \n".format(options.batch_size))

    file.write("scheduler.thrift.ports = {} \n".format(options.scheduler_ports))
    file.write("scheduler.thrift.threads = {} \n".format(options.scheduler_thrift_threads))

    file.write("datastore.thrift.ports = {} \n".format(options.data_store_ports))
    file.write("datastore.thrift.threads = {} \n".format(options.data_store_thrift_threads))

    file.write("node.monitor.thrift.ports = {} \n".format(options.node_monitor_ports))
    file.write("node.monitor.thrift.threads = {} \n".format(options.node_monitor_thrift_threads))

    file.write("node.enqueue.thrift.ports = {} \n".format(options.node_enqueue_ports))
    file.write("node.enqueue.thrift.threads = {} \n".format(options.node_enqueue_thrift_threads))
    file.write("replay.with.delay = {} \n".format(options.replay_with_delay))
    file.write("replay.with.disk = {} \n".format(options.replay_with_disk))
    file.write("scheduler.num.tasks.update = {} \n".format(options.scheduler_num_tasks_update))

    file.write("trace.num.thread.concurrent.submitted.tasks = {} \n"
               .format(options.num_thread_concurrent_submitted_tasks))

    file.write("network.interface = {} \n".format(options.network_interface))
    file.write("dodoor.cpu.weight = {} \n".format(options.cpu_weight))
    file.write("dodoor.memory.weight = {} \n".format(options.memory_weight))
    file.write("dodoor.disk.weight = {} \n".format(options.disk_weight))
    file.write("dodoor.total.pending.duration.weight = {} \n".format(options.duration_weight))

    # prequal options
    file.write("prequal.probe.ratio = {} \n".format(options.prequal_probe_ratio))
    file.write("prequal.probe.pool.size = {} \n".format(options.prequal_probe_pool_size))
    file.write("prequal.rif.quantile = {} \n".format(options.prequal_rif_quantile))
    file.write("prequal.probe.reuse.budget = {} \n".format(options.prequal_probe_reuse_budget))
    file.write("prequal.probe.remove.interval.ms = {} \n".format(options.prequal_probe_remove_interval_ms))
    file.write("task.replay.time.scale = {} \n".format(options.task_replay_time_scale))

    file.close()


def write_ip_port(file, lines, prefix):
    socket_addresses = ""
    for line in lines:
        tokens = line.strip().split(":")
        if len(tokens) != 2:
            return
        ip = tokens[1]
        socket_addresses += ip + ","
    socket_addresses = socket_addresses[:-1]
    file.write(prefix + " = " + socket_addresses + "\n")


if __name__ == "__main__":
    main()
