/*
 * Copyright 2024 University of Cambridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.cam.dodoor;

public class DodoorConf {
    // Values: "debug", "info", "warn", "error", "fatal"
    public final static String LOG_LEVEL = "log_level";


    /**
     * Scheduler config
     */
    public final static String SCHEDULER_TYPE = "scheduler.type";
    /** Current supported scheduling algorithm*/
    public final static String DODOOR_SCHEDULER = "dodoor";
    public final static String SPARROW_SCHEDULER = "sparrow";
    public final static String CACHED_SPARROW_SCHEDULER = "cached_sparrow_scheduler";
    public final static String RANDOM_SCHEDULER = "random";

    public final static String SCHEDULER_THRIFT_PORTS = "scheduler.thrift.ports";
    public final static int DEFAULT_SCHEDULER_THRIFT_PORT = 20503;
    public final static String SCHEDULER_THRIFT_THREADS =
            "scheduler.thrift.threads";
    // Listen port for the state store --> scheduler interface
    public final static int DEFAULT_SCHEDULER_THRIFT_THREADS = 10;

    // beta value for 1 + beta process
    public final static String BETA = "dodoor.beta";
    public final static double DEFAULT_BETA = 0.75;



    /**
     * Data Store Config
     */
    public final static String DATA_STORE_THRIFT_PORTS = "datastore.thrift.ports";
    public final static String DATA_STORE_THRIFT_THREADS =
            "datastore.thrift.threads";

    public final static int DEFAULT_DATA_STORE_THRIFT_PORT = 20510;
    public final static int DEFAULT_DATA_STORE_THRIFT_THREADS = 4;
    public final static String BATCH_SIZE = "dodoor.batch_size";
    public final static int DEFAULT_BATCH_SIZE = 1024;


    /**
     * Node Config
     */
    /* List of ports corresponding to node monitors (backend interface) this daemon is
     * supposed to run. In most deployment scenarios this will consist of a single port,
     * or will be left unspecified in favor of the default port. */
    public final static String NODE_MONITOR_THRIFT_PORTS = "node.monitor.thrift.ports";
    public final static int DEFAULT_NODE_MONITOR_THRIFT_PORT = 20501;
    public final static String NM_THRIFT_THREADS = "node.monitor.thrift.threads";
    public final static int DEFAULT_NM_THRIFT_THREADS = 4;

    /* List of ports corresponding to node monitors (internal interface) this daemon is
     * supposed to run. In most deployment scenarios this will consist of a single port,
     * or will be left unspecified in favor of the default port. */
    public final static String NODE_ENQUEUE_THRIFT_PORTS = "node.enqueue.thrift.ports";
    public final static int DEFAULT_NODE_ENQUEUE_THRIFT_PORT = 20502;

    public final static String INTERNAL_THRIFT_THREADS =
            "internal.thrift.threads";
    public final static int DEFAULT_NM_INTERNAL_THRIFT_THREADS = 1;


    /** Type of task scheduler to use on node monitor. Values: "fifo," "round_robin, " "priority" to be implemented.
     **/
    public final static String NM_TASK_SCHEDULER_TYPE = "node_monitor.task_scheduler";

    public final static String NUM_SLOTS = "node_monitor.num_slots";
    public final static int DEFAULT_NUM_SLOTS = 4;

    public final static String SYSTEM_MEMORY = "system.memory";
    public final static int DEFAULT_SYSTEM_MEMORY = 1024;

    public final static String SYSTEM_CORES = "system.cores";
    public final static int DEFAULT_SYSTEM_CORES = 4;

    public final static String SYSTEM_DISK = "system.disk";
    public final static int DEFAULT_SYSTEM_DISK = 10240;

    /** The hostname of this machine. */
    public final static String HOSTNAME = "hostname";
    public final static String STATIC_NODE = "static.node";
    public final static String STATIC_SCHEDULER = "static.scheduler";
    public final static String STATIC_DATA_STORE = "static.datastore";


    /** Config for tracing and monitoring. */
    public final static String TRACKING_ENABLED = "tracking.enabled";
    public final static boolean DEFAULT_TRACKING_ENABLED = true;
    public final static String TRACKING_INTERVAL_IN_SECONDS = "tracking.interval.seconds";
    public final static int DEFAULT_TRACKING_INTERVAL = 10;

    public final static String NODE_NUM_TASKS_TO_UPDATE = "node.num.tasks.update";
    public final static int  DEFAULT_NODE_NUM_TASKS_TO_UPDATE = 8;

    public final static String SCHEDULER_NUM_TASKS_TO_UPDATE = "scheduler.num.tasks.update";
    public final static int  DEFAULT_SCHEDULER_NUM_TASKS_TO_UPDATE = 10;

    public final static String DATA_STORE_METRICS_REGISTRY = "datastore.metrics.registry";
    public final static String DATA_STORE_METRICS_LOG_FILE = "datastore.metrics.log.file";
    public final static String DEFAULT_DATA_STORE_METRICS_LOG_FILE = "dodoor_datastore_metrics.log";
    public final static String DATA_STORE_METRICS_NUM_MESSAGES = "datastore.metrics.num.messages";
    public final static String DATA_STORE_METRICS_GET_REQUEST_RATE = "datastore.metrics.request.get.rate";
    public final static String DATA_STORE_METRICS_OVERRIDE_REQUEST_RATE = "datastore.metrics.request.override.rate";
    public final static String DATA_STORE_METRICS_ADD_REQUEST_RATE = "datastore.metrics.request.add.rate";


    public final static String SCHEDULER_METRICS_REGISTRY = "scheduler.metrics.registry";
    public final static String SCHEDULER_METRICS_LOG_FILE = "scheduler.metrics.log.file";
    public final static String DEFAULT_SCHEDULER_METRICS_LOG_FILE = "dodoor_scheduler_metrics.log";
    public final static String SCHEDULER_METRICS_END_TO_END_TASK_SCHEDULING_LATENCY_HISTOGRAMS =
            "scheduler.metrics.tasks.e2e.scheduling.latency.histograms";
    public final static String SCHEDULER_METRICS_TASK_RATE = "scheduler.metrics.tasks.rate";
    public final static String SCHEDULER_METRICS_LOAD_UPDATE_RATE = "scheduler.metrics.load.update.rate";
    public final static String SCHEDULER_METRICS_NUM_MESSAGES = "scheduler.metrics.num.messages";

    /**
     * Node Monitor Config
     */
    public final static String NODE_METRICS_REGISTRY = "node.metrics.registry";
    public final static String NODE_METRICS_LOG_FILE = "node.metrics.log.file";
    public final static String DEFAULT_NODE_METRICS_LOG_FILE = "dodoor_node_metrics.log";
    public final static String NODE_METRICS_WAITING_TASKS = "node.metrics.tasks.waiting.count";
    public final static String NODE_METRICS_FINISHED_TASKS = "node.metrics.tasks.finished.count";
    public final static String NODE_METRICS_TASKS_WAIT_TIME_HISTOGRAMS = "node.metrics.tasks.wait.time.histograms";
    public final static String NODE_METRICS_TASKS_RATE = "node.metrics.tasks.rate";
    public final static String NODE_METRICS_NUM_MESSAGES = "node.metrics.num.messages";



    public final static String DODOOR_NUM_SCHEDULER_CLIENTS_PER_PORT = "dodoor.scheduler.clients.count";
    public final static int DEFAULT_DODOOR_NUM_SCHEDULER_CLIENTS_PER_PORT = 8;

    public final static String REPLAY_WITH_TIMELINE_DELAY = "replay.with.delay";
    public final static boolean DEFAULT_REPLAY_WITH_TIMELINE_DELAY = true;

    // Consider some trace like GCP is not recording the disk resources. We may disable this for better comparison across traces.
    public final static String REPLAY_WITH_DISK = "replay.with.disk";
    public final static boolean DEFAULT_REPLAY_WITH_DISK = false;
}
