package edu.cam.dodoor;

import edu.cam.dodoor.datastore.DataStoreThrift;
import edu.cam.dodoor.node.NodeThrift;
import edu.cam.dodoor.scheduler.SchedulerThrift;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class ServiceDaemon {
    public final static Level DEFAULT_LOG_LEVEL = Level.DEBUG;

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("c", "configuration file (required)").
                withRequiredArg().ofType(String.class);
        parser.accepts("host", "host configurations file (required)").
                withRequiredArg().ofType(String.class);
        parser.accepts("task", "task configurations file (required)").
                withRequiredArg().ofType(String.class);
        parser.accepts("s", "If contains scheduler or not").
                withRequiredArg().ofType(Boolean.class);
        parser.accepts("d", "If contains a datastore service or not").
                withRequiredArg().ofType(Boolean.class);
        parser.accepts("n", "If contains a node service or not").
                withRequiredArg().ofType(Boolean.class);
        parser.accepts("help", "print help statement");
        OptionSet options = parser.parse(args);

        if (options.has("help") || !options.has("c")) {
            parser.printHelpOn(System.out);
            System.exit(-1);
        }

        // Set up a simple configuration that logs on the console.
        BasicConfigurator.configure();

        String configFile = (String) options.valueOf("c");
        Configuration conf = new PropertiesConfiguration(configFile);

        String hostConfigFile = (String) options.valueOf("host");
        JSONObject hostConfig = new JSONObject(
                Files.readString(Paths.get(hostConfigFile)));

        String taskConfigFile = (String) options.valueOf("task");
        JSONObject taskConfig = new JSONObject(
                Files.readString(Paths.get(taskConfigFile)));

        Boolean isScheduler = (Boolean) options.valueOf("s");
        Boolean isDataStore = (Boolean) options.valueOf("d");
        Boolean isNode = (Boolean) options.valueOf("n");

        if (!isScheduler && !isDataStore && !isNode) {
            throw new ConfigurationException("At least one service must be specified");
        }

        ServiceDaemon daemon = new ServiceDaemon();
        daemon.initialize(conf, hostConfig, taskConfig, isScheduler, isDataStore, isNode);
    }

    private void initialize(Configuration config,
                            JSONObject hostConfig,
                            JSONObject taskConfig,
                            boolean isScheduler,
                            boolean isDataStore,
                            boolean isNode) throws Exception{
        Level logLevel = Level.toLevel(config.getString(DodoorConf.LOG_LEVEL, ""),
                DEFAULT_LOG_LEVEL);
        Logger.getRootLogger().setLevel(logLevel);

        if (isNode) {
            // current nmPort and nePort are only supported to be one in each node
            JSONObject nodeConfig = hostConfig.getJSONObject(DodoorConf.NODE_SERVICE_NAME);
            int nmPorts = nodeConfig.getInt(DodoorConf.NODE_MONITOR_THRIFT_PORTS);
            int nePorts = nodeConfig.getInt(DodoorConf.NODE_ENQUEUE_THRIFT_PORTS);
            new NodeThrift().initialize(config, nmPorts, nePorts, hostConfig, taskConfig);
        }

        if (isDataStore) {
            boolean logKicked = false;
            JSONArray dataStorePortsArray = hostConfig.getJSONObject(DodoorConf.DATA_STORE_SERVICE_NAME)
                    .getJSONArray(DodoorConf.SERVICE_PORT_LIST_KEY);
            for (int i = 0; i < dataStorePortsArray.length(); i++) {
                String dataStorePort = dataStorePortsArray.getString(i);
                DataStoreThrift dataStore = new DataStoreThrift();
                dataStore.initialize(config, Integer.parseInt(dataStorePort), logKicked, hostConfig);
                logKicked = true;
            }
        }

        if (isScheduler) {
            JSONArray schedulerPorts = hostConfig.getJSONObject(DodoorConf.SCHEDULER_SERVICE_NAME)
                    .getJSONArray(DodoorConf.SERVICE_PORT_LIST_KEY);
            boolean logKicked = false;
            for (int i = 0; i < schedulerPorts.length(); i++) {
                String schedulerPort = schedulerPorts.getString(i);
                SchedulerThrift scheduler = new SchedulerThrift();
                scheduler.initialize(config, Integer.parseInt(schedulerPort), logKicked, hostConfig,
                        taskConfig);
                logKicked = true;
            }
        }
    }
}
