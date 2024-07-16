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
    public final static String DODOOR_SCHEDULER = "Dodoor";
    public final static String SPARROW_SCHEDULER = "Sparrow";
    public final static String CACHED_SPARROW_SCHEDULER = "CachedSparrowScheduler";

    public final static String SCHEDULER_THRIFT_PORT = "scheduler.thrift.port";
    public final static int DEFAULT_SCHEDULER_THRIFT_PORT = 20503;
    public final static String NUM_SCHEDULER =
            "scheduler.count";
    // Listen port for the state store --> scheduler interface
    public final static int DEFAULT_NUM_SCHEDULER = 10;

    // beta value for 1 + beta process
    public final static String BETA = "scheduler.beta";
    public final static double DEFAULT_BETA = 0.75;



    /**
     * Data Store Config
     */
    public final static String DATA_STORE_THRIFT_PORT = "datastore.thrift.port";
    public final static String DATA_STORE_THRIFT_THREADS =
            "scheduler.thrift.threads";

    public final static int DEFAULT_DATA_STORE_THRIFT_PORT = 20510;
    public final static int DEFAULT_DATA_STORE_THRIFT_THREADS = 1;
    public final static String BATCH_SIZE = "static.batch_size";
    public final static int DEFAULT_BATCH_SIZE = 1024;


    /**
     * Node Config
     */
    /* List of ports corresponding to node monitors (backend interface) this daemon is
     * supposed to run. In most deployment scenarios this will consist of a single port,
     * or will be left unspecified in favor of the default port. */
    public final static String NM_THRIFT_PORTS = "agent.thrift.ports";
    public final static int DEFAULT_NM_THRIFT_PORT = 20501;
    public final static int DEFAULT_INTERNAL_THRIFT_PORT = 20502;

    /* List of ports corresponding to node monitors (internal interface) this daemon is
     * supposed to run. In most deployment scenarios this will consist of a single port,
     * or will be left unspecified in favor of the default port. */
    public final static String INTERNAL_THRIFT_PORTS = "internal_agent.thrift.ports";

    public final static String NM_THRIFT_THREADS = "agent.thrift.threads";
    public final static int DEFAULT_NM_THRIFT_THREADS = 4;
    public final static int DEFAULT_NM_INTERNAL_THRIFT_THREADS = 8;
    public final static String INTERNAL_THRIFT_THREADS =
            "internal_agent.thrift.threads";


    /** Type of task scheduler to use on node monitor. Values: "fifo," "round_robin, " "priority" to be implemented.
     **/
    public final static String NM_TASK_SCHEDULER_TYPE = "node_monitor.task_scheduler";

    public final static String NUM_SLOTS = "node_monitor.num_slots";
    public final static int DEFAULT_NUM_SLOTS = 4;

    public final static String SYSTEM_MEMORY = "system.memory";
    public final static int DEFAULT_SYSTEM_MEMORY = 1024;

    public final static String SYSTEM_CPUS = "system.cpus";
    public final static int DEFAULT_SYSTEM_CPUS = 4;

    public final static String SYSTEM_DISK = "system.disk";
    public final static int DEFAULT_SYSTEM_DISK = 10240;

    /** The hostname of this machine. */
    public final static String HOSTNAME = "hostname";
    public final static String STATIC_NODE_MONITORS = "static.node_monitors";
    public final static String STATIC_SCHEDULER = "static.scheduler";
    public final static String STATIC_DATA_STORE = "static.datastore";


    /** Config for tracing and monitoring. */
    public final static String TRACKING_ENABLED = "tracking.enabled";
    public final static boolean DEFAULT_TRACKING_ENABLED = false;
    public final static String TRACKING_INTERVAL_IN_MS = "tracking.interval";
    public final static int DEFAULT_TRACKING_INTERVAL = 10000;

    public final static String METRICS_LOG_FILE = "metrics.log.file";
    public final static String DEFAULT_METRICS_LOG_FILE = "dodoor_metrics.log";
}
