from optparse import OptionParser


def parse_args():
    parser = OptionParser(usage="dodoor-config [options]" +
                                "\n\n generate the configuration file for dodoor experiments")
    parser.add_option("-n", "--nodes-file",
                      help="Inject the host ip addresses of nodes into the config file from the host files")
    parser.add_option("-s", "--scheduler-file",
                      help="Inject the scheduler ip address into the config file from the scheduler file")
    parser.add_option("-d", "--data-store-file",
                      help="Inject the data store ip address into the config file from the data store file")
    parser.add_option("-o", "--output", default="./config.conf",
                      help="The output path of generated configuration file")
    parser.add_option("--node-ports", default="20501",
                      help="The port numbers of the node, passing multiple options separated by comma")
    parser.add_option("--scheduler-ports", default="20503",
                      help="The port numbers of the scheduler, passing multiple options separated by comma")
    parser.add_option("--data-store-ports", default="20510",
                      help="The port number of the data store, passing multiple options separated by comma")
    parser.add_option("--internal-ports", default="20502",
                      help="The port number of the internal service to enqueue and dequeue the tasks")
    parser.add_option("--scheduler-thrift-threads", default=10,
                      help="The number of threads running in scheduler service to listen to the thrift requests")
    parser.add_option("--node-thrift-threads", default=4,
                      help="The number of threads running in node monitor to listen to the thrift requests")
    parser.add_option("--data-store-thrift-threads", default=4,
                      help="The number of threads running in data store to listen to the thrift requests")
    parser.add_option("--internal-thrift-threads", default=4,
                      help="The number of threads running in internal service to listen to the thrift requests")
    parser.add_option("-t", "--trace-file", default=None,
                      help="The trace file to be used in the experiment, None means no tracking enabled")
    parser.add_option("--tracking-interval", default=1000,
                      help="The interval of tracking the system status")
    parser.add_option("--cores", default=24,
                      help="The number of available cores in the system")
    parser.add_option("--memory", default=20480,
                      help="The amount of memory in Mb in the system")
    parser.add_option("--disk", default=40960,
                      help="The amount of disk in Mb in the system")
    parser.add_option("--num-slots", default=4,
                      help="The number of slots in each node to run the tasks in parallel")
    parser.add_option("--beta", default=0.6,
                      help="The beta value used for 1 + beta process for random scheduling")
    parser.add_option("--batch-size", default=1024,
                      help="The batch size of the tasks to be scheduled by dodoor scheduler")
    return parser.parse_args()


# adding options
def main():
    # add options
    (options, args) = parse_args()
    print("Generating configuration file at " + options.output)

    config_path = options.output
    file = open(config_path, "w")

    with open(options.nodes_file, "r") as f:
        write_ip_port(file, f.readlines(), "static.node_monitors", options.node_ports)

    with open(options.scheduler_file, "r") as f:
        write_ip_port(file, f.readlines(), "static.scheduler", options.scheduler_ports)

    with open(options.data_store_file, "r") as f:
        write_ip_port(file, f.readlines(), "static.data_store", options.data_store_ports)

    if options.trace_file:
        file.write("tracking.enabled  = true")
        file.write("tracking.interval = " + options.tracking_interval + "\n")
        file.write("metrics.log.file = " + options.trace_file + "\n")

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

    file.write("node.thrift.ports = {} \n".format(options.node_ports))
    file.write("node.thrift.threads = {} \n".format(options.node_thrift_threads))

    file.write("internal.thrift.ports = {} \n".format(options.internal_ports))
    file.write("internal.thrift.threads = {} \n".format(options.internal_thrift_threads))
    file.close()


def write_ip_port(file, lines, prefix, ports):
    socket_addresses = ""
    ports = ports.split(",")
    for line in lines:
        tokens = line.strip().split(":")
        if len(tokens) != 2:
            return
        ip = tokens[1]
        for port in ports:
            socket_addresses += ip + ":" + port + ","
        socket_addresses = socket_addresses[:-1]
    file.write(prefix + " = " + socket_addresses + "\n")


if __name__ == "__main__":
    main()