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
    private static final Logger LOG = Logger.getLogger(TaskTracePlayer.class);

    public static void main(String[] args) throws ConfigurationException, TException, IOException {
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

        for (String line : allLines) {
            String[] parts = line.split(",");
            String taskId = parts[0];
            int cores = Integer.parseInt(parts[1]);
            long memory = Long.parseLong(parts[2]);
            long disks = replayWithDisk? Long.parseLong(parts[3]):0;
            long durationInMs = Long.parseLong(parts[4]);
            long startTime = Long.parseLong(parts[5]);

            if (addDelay) {
                long currentTime = System.currentTimeMillis();
                long delay = startTime - (currentTime - globalStartTime);
                if (delay > 0) {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else if (delay < 0) {
                    LOG.warn(String.format("Task %s is already late by %d ms", taskId, -delay));
                }
            }
            client.submitTask(taskId, cores, memory, disks, durationInMs);
        }
    }
}
