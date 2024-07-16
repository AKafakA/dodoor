package edu.cam.dodoor.client;

import edu.cam.dodoor.DodoorConf;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class TaskTracePlayer {
    private final Logger LOG = Logger.getLogger(TaskTracePlayer.class);

    private class TaskLaunchRunnable implements Runnable {
        //        private int requestId;
        private final String _taskId;
        private final int _cores;
        private final long _memory;
        private final long _disks;
        private final long _durationInMs;
        private final long _startTime;
        private final DodoorClient _client;
        private final long _globalStartTime;

        public TaskLaunchRunnable(String taskId, int cores, long memory,
                                  long disks, long durationInMs, long startTime,
                                  DodoorClient client, long globalStartTime) {
            _taskId = taskId;
            _cores = cores;
            _memory = memory;
            _disks = disks;
            _durationInMs = durationInMs;
            _startTime = startTime;
            _client = client;
            _globalStartTime = globalStartTime;
        }

        @Override
        public void run(){
            // Generate tasks in the format expected by Sparrow. First, pack task parameters.
            long start = System.currentTimeMillis() - _globalStartTime;
            if (start < _startTime) {
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
            long end = System.currentTimeMillis();
            LOG.debug("Scheduling request duration " + (end - start));
        }
    }

    public static void main(String[] args) throws ConfigurationException, TException, IOException {
        OptionParser parser = new OptionParser();
        parser.accepts("f", "trace files").
                withRequiredArg().ofType(String.class);
        parser.accepts("c", "configuration file (required)").
                withRequiredArg().ofType(String.class);
        parser.accepts("help", "print help statement");
        OptionSet options = parser.parse(args);


        DodoorClient client = new DodoorClient();
        Configuration conf = new PropertiesConfiguration();

        if (options.has("c")) {
            String configFile = (String) options.valueOf("c");
            conf = new PropertiesConfiguration(configFile);
        }
        int schedulerPort = conf.getInt(DodoorConf.SCHEDULER_THRIFT_PORT,
                DodoorConf.DEFAULT_SCHEDULER_THRIFT_PORT);
        client.initialize(new InetSocketAddress("localhost", schedulerPort));
        long globalStartTime = System.currentTimeMillis();

        String traceFile = (String) options.valueOf("f");
        List<String> allLines = Files.readAllLines(Paths.get(traceFile));
        for (String line : allLines) {
            String[] parts = line.split(",");
            String taskId = parts[0];
            int cores = Integer.parseInt(parts[1]);
            long memory = Long.parseLong(parts[2]);
            long disks = Long.parseLong(parts[3]);
            long durationInMs = Long.parseLong(parts[4]);
            long startTime = Long.parseLong(parts[5]);
            (new TaskTracePlayer()).new TaskLaunchRunnable(taskId, cores, memory, disks, durationInMs,
                    startTime, client, globalStartTime).run();
        }

    }

}
