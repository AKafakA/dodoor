package edu.cam.dodoor.client;

import edu.cam.dodoor.DodoorConf;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
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
        private final long _globalStartTime;
        private final boolean _addTimelineDelay;

        public TaskLaunchRunnable(String taskId, int cores, long memory,
                                  long disks, long durationInMs, long startTime,
                                  DodoorClient client, long globalStartTime, boolean addTimelineDelay) {
            _taskId = taskId;
            _cores = cores;
            _memory = memory;
            _disks = disks;
            _durationInMs = durationInMs;
            _startTime = startTime;
            _client = client;
            _globalStartTime = globalStartTime;
            _addTimelineDelay = addTimelineDelay;
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
                _client.submitTask(_taskId, _cores, _memory, _disks, _durationInMs);
            } catch (TException e) {
                LOG.error("Scheduling request failed!", e);
            }
        }
    }

    public static void main(String[] args) throws ConfigurationException, TException, IOException {
        BasicConfigurator.configure();
        OptionParser parser = new OptionParser();
        parser.accepts("f", "trace files").
                withRequiredArg().ofType(String.class);
        parser.accepts("c", "configuration file (required)").
                withRequiredArg().ofType(String.class);
        parser.accepts("help", "print help statement");
        OptionSet options = parser.parse(args);


        DodoorClient client = new DodoorClient();
        Configuration config = new PropertiesConfiguration();

        if (options.has("c")) {
            String configFile = (String) options.valueOf("c");
            config = new PropertiesConfiguration(configFile);
        }

        String[] schedulerPorts = config.getStringArray(DodoorConf.SCHEDULER_THRIFT_PORTS);

        if (schedulerPorts.length == 0) {
            schedulerPorts = new String[]{Integer.toString(DodoorConf.DEFAULT_SCHEDULER_THRIFT_PORT)};
        }
        InetSocketAddress[] schedulerAddresses = new InetSocketAddress[schedulerPorts.length];
        for (int i = 0; i < schedulerPorts.length; i++) {
            schedulerAddresses[i] = new InetSocketAddress("localhost", Integer.parseInt(schedulerPorts[i]));
        }
        client.initialize(schedulerAddresses, config);
        long globalStartTime = System.currentTimeMillis();

        String traceFile = (String) options.valueOf("f");
        List<String> allLines = Files.readAllLines(Paths.get(traceFile));

        boolean addDelay = config.getBoolean(DodoorConf.REPLAY_WITH_TIMELINE_DELAY,
                DodoorConf.DEFAULT_REPLAY_WITH_TIMELINE_DELAY);

        boolean replayWithDisk = config.getBoolean(DodoorConf.REPLAY_WITH_DISK,
                DodoorConf.DEFAULT_REPLAY_WITH_DISK);
        int numThreads = config.getInt(DodoorConf.NUM_THREAD_CONCURRENT_SUBMITTED_TASKS,
                DodoorConf.DEFAULT_NUM_THREAD_CONCURRENT_SUBMITTED_TASKS);
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (String line : allLines) {
            String[] parts = line.split(",");
            String taskId = parts[0];
            int cores = Integer.parseInt(parts[1]);
            long memory = Long.parseLong(parts[2]);
            long disks = replayWithDisk? Long.parseLong(parts[3]):0;
            long durationInMs = Long.parseLong(parts[4]);
            long startTime = Long.parseLong(parts[5]);
            executor.submit(new TaskLaunchRunnable(taskId, cores, memory, disks, durationInMs,
                    startTime, client, globalStartTime, addDelay));
        }
    }
}
