from optparse import OptionParser


def parse_args():
    parser = OptionParser(usage="dodoor-config [options]" +
                                "\n\n generate the configuration file for dodoor experiments")
    parser.add_option("--scheduler-type", default="dodoor",
                      help="The type of scheduler to be used in the experiment")
    parser.add_option("--scheduler-data-store-colocated", default=True,
                      help="Whether the scheduler and data store are colocated on the same host")
    parser.add_option("-o", "--output", default="./config.conf",
                      help="The output path of generated configuration file")
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
    parser.add_option("--prequal_probe_rate", default=3,
                      help="The rate of probe tasks to the total tasks in the system")
    parser.add_option("--prequal_probe_pool_size", default=16,
                        help="The pool size of probe tasks to be scheduled")
    parser.add_option("--prequal_rif_quantile", default=0.84,
                        help="The quantile of RIF to be used in the prequal scheduler")
    parser.add_option("--prequal_probe_age_budget_ms", default=1000,
                        help="The age budget of probed results in the prequal scheduler. The probed results older than "
                             "this value will be removed from the probed results pool")
    parser.add_option("--prequal_delta", default=1,
                      help="The delta value used in the prequal scheduler for probe reuse budget")
    parser.add_option("--prequal_probe_delete_rate", default=1,
                      help="The probe delete rate used in the prequal scheduler for probe reuse budget")
    parser.add_option("--task_replay_time_scale", default=1.0,
                      help="The time scale to replay the tasks in the trace, used to speed up the replay process for "
                           "debugging scheduler performances")
    parser.add_option("--cluster_avg_load", default=0.0,
                      help="The average load of the cluster, used to scale the load score of the tasks in the trace")
    parser.add_option("--host_load_beta_k", default=10,
                        help="The beta value used to scale the load ratio of the tasks in the trace")
    return parser.parse_args()


# adding options
def main():
    # add options
    (options, args) = parse_args()
    print("Generating configuration file at " + options.output)

    config_path = options.output
    file = open(config_path, "w")

    scheduler_type = options.scheduler_type
    file.write("scheduler.type = {} \n".format(scheduler_type))

    if options.trace_enabled:
        file.write("tracking.enabled = true \n")
        file.write("tracking.interval.seconds = {} \n".format(options.tracking_interval))
        file.write("node.metrics.log.file = {} \n".format(options.node_trace_file))
        file.write("datastore.metrics.log.file = {} \n".format(options.datastore_trace_file))
        file.write("scheduler.metrics.log.file = {} \n".format(options.scheduler_trace_file))

    file.write("dodoor.beta = {} \n".format(options.beta))
    file.write("dodoor.batch_size = {} \n".format(options.batch_size))

    file.write("scheduler.thrift.threads = {} \n".format(options.scheduler_thrift_threads))

    file.write("datastore.thrift.threads = {} \n".format(options.data_store_thrift_threads))

    file.write("node.monitor.thrift.threads = {} \n".format(options.node_monitor_thrift_threads))

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
    file.write("prequal.probe.rate = {} \n".format(options.prequal_probe_rate))
    file.write("prequal.probe.pool.size = {} \n".format(options.prequal_probe_pool_size))
    file.write("prequal.rif.quantile = {} \n".format(options.prequal_rif_quantile))
    file.write("prequal.delta = {} \n".format(options.prequal_delta))
    file.write("prequal.probe.delete.rate = {} \n".format(options.prequal_probe_delete_rate))
    file.write("prequal.probe.age.budget.ms = {} \n".format(options.prequal_probe_age_budget_ms))

    file.write("task.replay.time.scale = {} \n".format(options.task_replay_time_scale))

    file.write("cluster.average.load = {} \n".format(options.cluster_avg_load))
    file.write("host.load.beta.k = {} \n".format(options.host_load_beta_k))

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
