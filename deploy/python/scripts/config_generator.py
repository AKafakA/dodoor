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
    parser.add_option("--scheduler-ports", default="20503",
                      help="The port numbers of the scheduler, passing multiple options separated by comma. "
                           "Each port will be used by a individual scheduler. So, the number of ports should be equal "
                           "to the number of schedulers per host.")
    parser.add_option("--data-store-ports", default="20510",
                      help="The port number of the data store, passing multiple options separated by comma, "
                           "same as scheduler ports to create multiple data stores instances")
    parser.add_option("--node-enqueue-ports", default="20502",
                      help="The port number of the internal service to enqueue and dequeue the tasks")
    parser.add_option("--scheduler-thrift-threads", default=10,
                      help="The number of threads running in scheduler service to listen to the thrift requests")
    parser.add_option("--node-monitor-thrift-threads", default=4,
                      help="The number of threads running in node monitor to listen to the thrift requests")
    parser.add_option("--data-store-thrift-threads", default=4,
                      help="The number of threads running in data store to listen to the thrift requests")
    parser.add_option("--node-enqueue-thrift-threads", default=4,
                      help="The number of threads running in internal service to listen to the thrift requests")
    parser.add_option("-t", "--trace-enabled",
                      default=True, help="whether to enable the trace of the system status")
    parser.add_option("--node_trace-file", default="dodoor_node_metrics.log",
                      help="The trace file of node service to be used in the experiment")
    parser.add_option("--datastore_trace-file", default="dodoor_datastore_metrics.log",
                      help="The trace file of datastore service to be used in the experiment")
    parser.add_option("--scheduler_trace-file", default="dodoor_scheduler_metrics.log",
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
    parser.add_option("--node-num-tasks-update", default=8,
                      help="The number of tasks to be completed before nodes overrides its loads in datastore")
    parser.add_option("--scheduler-num-tasks-update", default=8,
                      help="The number of tasks to be scheduled before scheduler update its load views in datastore")
    parser.add_option("--replay_with_delay", default=True,
                      help="Whether to replay the trace following the provided timeline and "
                           "delay the tasks kicking off until the time comes.")
    parser.add_option("--replay_with_disk", default=False,
                      help="Whether to replay the trace with disks requirements or just ignore it.")

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

    file.write("node.num.tasks.update = {} \n".format(options.node_num_tasks_update))
    file.write("scheduler.num.tasks.update = {} \n".format(options.scheduler_num_tasks_update))

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
