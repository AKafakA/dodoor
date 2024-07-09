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
    public final static String SCHEDULER_THRIFT_PORT = "scheduler.thrift.port";
    public final static String SCHEDULER_THRIFT_THREADS =
            "scheduler.thrift.threads";
    // Listen port for the state store --> scheduler interface
    public final static String SCHEDULER_STATE_THRIFT_PORT = "scheduler.state.thrift.port";
    public final static String SCHEDULER_STATE_THRIFT_THREADS =
            "scheduler.state.thrift.threads";



    /**
     * Data Store Config
     */
    public final static String DATA_STORE_THRIFT_PORT = "datastore.thrift.port";
    public final static String DATA_STORE_THRIFT_THREADS =
            "scheduler.thrift.threads";

    public final static int DEFAULT_DATA_STORE_THRIFT_PORT = 53001;
    public final static int DEFAULT_DATA_STORE_THRIFT_THREADS = 1;

    /**
     * Whether the scheduler should cancel outstanding reservations when all of a job's tasks have
     * been scheduled.  Should be set to "true" or "false".
     */
    public final static String CANCELLATION = "cancellation";
    public final static boolean DEFAULT_CANCELLATION = true;

    /* List of ports corresponding to node monitors (backend interface) this daemon is
     * supposed to run. In most deployment scenarios this will consist of a single port,
     * or will be left unspecified in favor of the default port. */
    public final static String NM_THRIFT_PORTS = "agent.thrift.ports";

    /* List of ports corresponding to node monitors (internal interface) this daemon is
     * supposed to run. In most deployment scenarios this will consist of a single port,
     * or will be left unspecified in favor of the default port. */
    public final static String INTERNAL_THRIFT_PORTS = "internal_agent.thrift.ports";

    public final static String NM_THRIFT_THREADS = "agent.thrift.threads";
    public final static String INTERNAL_THRIFT_THREADS =
            "internal_agent.thrift.threads";
    /** Type of task scheduler to use on node monitor. Values: "fifo," "round_robin, " "priority." */
    public final static String NM_TASK_SCHEDULER_TYPE = "node_monitor.task_scheduler";

    public final static String NUM_SLOTS = "node_monitor.num_slots";
    public final static int DEFAULT_NUM_SLOTS = 4;

    public final static String SYSTEM_MEMORY = "system.memory";
    public final static int DEFAULT_SYSTEM_MEMORY = 1024;

    public final static String SYSTEM_CPUS = "system.cpus";
    public final static int DEFAULT_SYSTEM_CPUS = 4;

    public final static String SYSTEM_DISK = "system.disks";
    public final static int DEFAULT_SYSTEM_DISK = 10240;

    // Values: "standalone", "configbased." Only "configbased" works currently.
    public final static String DEPLYOMENT_MODE = "deployment.mode";
    public final static String DEFAULT_DEPLOYMENT_MODE = "production";

    /** The hostname of this machine. */
    public final static String HOSTNAME = "hostname";
    public final static String STATIC_NODE_MONITORS = "static.node_monitors";


    public static final String GET_TASK_PORT = "get_task.port";


    public final static String BATCH_SIZE = "static.batch_size";
    public final static int DEFAULT_BATCH_SIZE = 1024;


    public final static int DEFAULT_SCHEDULER_THRIFT_PORT = 20503;
    private final static int DEFAULT_SCHEDULER_THRIFT_THREADS = 8;
    public final static int DEFAULT_GET_TASK_PORT = 20507;
}
