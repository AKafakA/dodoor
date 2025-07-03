package edu.cam.dodoor.client;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.node.TaskTypeID;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.PoissonDistributionImpl;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class TaskTracePlayer {
    private static final Logger LOG = Logger.getLogger(TaskTracePlayer.class);

    private static class TaskLaunchRunnable implements Runnable {
        //        private int requestId;
        private final String _taskId;
        private final int _cores;
        private final long _memory;
        private final long _disks;
        private final long _durationInMs;
        private final long _startTime;
        private final DodoorClient _client;
        final long _globalStartTime;
        private final boolean _addTimelineDelay;
        private final String _taskType;

        public TaskLaunchRunnable(String taskId, int cores, long memory,
                                  long disks, long durationInMs, long startTime,
                                  DodoorClient client, long globalStartTime, boolean addTimelineDelay,
                                  String taskType) {
            _taskId = taskId;
            _cores = cores;
            _memory = memory;
            _disks = disks;
            _durationInMs = durationInMs;
            _startTime = startTime;
            _client = client;
            _globalStartTime = globalStartTime;
            _addTimelineDelay = addTimelineDelay;
            _taskType = taskType;
        }

        @Override
        public void run(){
            // Generate tasks in the format expected by Sparrow. First, pack task parameters.
            long start = System.currentTimeMillis() - _globalStartTime;
            if (start < _startTime && _addTimelineDelay) {
                try {
                    Thread.sleep(_startTime - start);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            try {
                _client.submitTask(_taskId, _cores, _memory, _disks, _durationInMs, _taskType);
            } catch (TException e) {
                LOG.error("Scheduling request failed!", e);
            }
        }
    }

    public static void main(String[] args) throws ConfigurationException, TException, IOException, MathException {
        BasicConfigurator.configure();
        OptionParser parser = new OptionParser();
        parser.accepts("f", "trace files").
                withRequiredArg().ofType(String.class);
        parser.accepts("c", "configuration file (required)").
                withRequiredArg().ofType(String.class);
        parser.accepts("host", "host configurations file (required)").
                withRequiredArg().ofType(String.class);
        parser.accepts("q", "QPS to replay the trace, -1 means no external QPS specified " +
                        "and the timeline in trace will be used").
                withRequiredArg().ofType(Double.class);
        parser.accepts("help", "print help statement");
        OptionSet options = parser.parse(args);



        DodoorClient client = new DodoorClient();
        Configuration staticConfig = new PropertiesConfiguration();

        if (options.has("c")) {
            String configFile = (String) options.valueOf("c");
            staticConfig = new PropertiesConfiguration(configFile);
        }
        String[] schedulerPorts;
        if (options.has("host")) {
            String hostConfigFile = (String) options.valueOf("host");
            JSONObject hostConfig = new JSONObject(Files.readString(Paths.get(hostConfigFile)));
            JSONObject schedulerConfig = hostConfig.getJSONObject(DodoorConf.SCHEDULER_SERVICE_NAME);
            JSONArray schedulerPortsJson = schedulerConfig.getJSONArray(DodoorConf.SERVICE_PORT_LIST_KEY);
            schedulerPorts = new String[schedulerPortsJson.length()];
            for (int i = 0; i < schedulerPortsJson.length(); i++) {
                schedulerPorts[i] = schedulerPortsJson.getString(i);
            }
        } else {
            schedulerPorts = new String[]{Integer.toString(DodoorConf.DEFAULT_SCHEDULER_THRIFT_PORT)};
        }

        InetSocketAddress[] schedulerAddresses = new InetSocketAddress[schedulerPorts.length];
        for (int i = 0; i < schedulerPorts.length; i++) {
            schedulerAddresses[i] = new InetSocketAddress("localhost", Integer.parseInt(schedulerPorts[i]));
        }
        client.initialize(schedulerAddresses, staticConfig);
        long globalStartTime = System.currentTimeMillis();

        String traceFile = (String) options.valueOf("f");
        List<String> allLines = Files.readAllLines(Paths.get(traceFile));

        boolean addDelay = staticConfig.getBoolean(DodoorConf.REPLAY_WITH_TIMELINE_DELAY,
                DodoorConf.DEFAULT_REPLAY_WITH_TIMELINE_DELAY);

        boolean replayWithDisk = staticConfig.getBoolean(DodoorConf.REPLAY_WITH_DISK,
                DodoorConf.DEFAULT_REPLAY_WITH_DISK);

        double taskReplyRate = staticConfig.getDouble(DodoorConf.TASK_REPLAY_TIME_SCALE,
                DodoorConf.DEFAULT_TASK_REPLAY_TIME_SCALE);

        double externalQPS = -1;
        if (options.has("q")) {
            externalQPS = (double) options.valueOf("q");
        }

        PoissonDistributionImpl poissonDistribution = new PoissonDistributionImpl(externalQPS);

        long startTime = 0;
        // Assume the trace file is (taskId, cores, memory, disks, durationInMs, startTime, taskType)
        for (String line : allLines) {
            String[] parts = line.split(",");
            String taskId = parts[0];
            int cores = Integer.parseInt(parts[1]);
            long memory = Long.parseLong(parts[2]);
            long disks = replayWithDisk? Long.parseLong(parts[3]):0;
            long durationInMs = Long.parseLong(parts[4]);
            if (externalQPS <= 0) {
                startTime = (long) Math.ceil(Long.parseLong(parts[5]) / taskReplyRate);
                if (startTime < 0) {
                    startTime = 0;
                }
            } else {
                //  follow poisson distribution under qps
                int waitTime = poissonDistribution.sample();
                startTime += waitTime;
            }
            String taskType = parts.length > 6 ? parts[6] : TaskTypeID.SIMULATED.toString();
            TaskLaunchRunnable task = new TaskLaunchRunnable(taskId, cores, memory, disks, durationInMs,
                    startTime, client, globalStartTime, addDelay, taskType);
            Thread t = new Thread(task);
            t.start();
        }
    }
}
