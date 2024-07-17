package edu.cam.dodoor;

import edu.cam.dodoor.datastore.DataStoreThrift;
import edu.cam.dodoor.nodemonitor.NodeMonitorThrift;
import edu.cam.dodoor.scheduler.SchedulerThrift;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class ServiceDaemon {
    public final static Level DEFAULT_LOG_LEVEL = Level.DEBUG;

    public static void main(String[] args) throws Exception {
        OptionParser parser = new OptionParser();
        parser.accepts("c", "configuration file (required)").
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

        Boolean isScheduler = (Boolean) options.valueOf("s");
        Boolean isDataStore = (Boolean) options.valueOf("d");
        Boolean isNode = (Boolean) options.valueOf("n");

        if (!isScheduler && !isDataStore && !isNode) {
            throw new ConfigurationException("At least one service must be specified");
        }

        ServiceDaemon daemon = new ServiceDaemon();
        daemon.initialize(conf, isScheduler, isDataStore, isNode);
    }

    private void initialize(Configuration config,
                            boolean isScheduler,
                            boolean isDataStore,
                            boolean isNode) throws Exception{
        Level logLevel = Level.toLevel(config.getString(DodoorConf.LOG_LEVEL, ""),
                DEFAULT_LOG_LEVEL);
        Logger.getRootLogger().setLevel(logLevel);

        if (isNode) {
            // Start as many node monitors as specified in config
            String[] nmPorts = config.getStringArray(DodoorConf.NM_THRIFT_PORTS);
            String[] inPorts = config.getStringArray(DodoorConf.INTERNAL_THRIFT_PORTS);

            if (nmPorts.length != inPorts.length) {
                throw new ConfigurationException(DodoorConf.NM_THRIFT_PORTS + " and " +
                        DodoorConf.INTERNAL_THRIFT_PORTS + " not of equal length");
            }
            if (nmPorts.length == 0) {
                (new NodeMonitorThrift()).initialize(config,
                        DodoorConf.DEFAULT_NM_THRIFT_PORT,
                        DodoorConf.DEFAULT_INTERNAL_THRIFT_PORT);
            }
            else {
                for (int i = 0; i < nmPorts.length; i++) {
                    (new NodeMonitorThrift()).initialize(config,
                            Integer.parseInt(nmPorts[i]), Integer.parseInt(inPorts[i]));
                }
            }
        }

        if (isScheduler) {
            String[] schedulerPorts = config.getStringArray(DodoorConf.SCHEDULER_THRIFT_PORTS);

            if (schedulerPorts.length == 0) {
                schedulerPorts = new String[]{Integer.toString(DodoorConf.DEFAULT_SCHEDULER_THRIFT_PORT)};
            }

            for (String schedulerPort : schedulerPorts) {
                SchedulerThrift scheduler = new SchedulerThrift();
                scheduler.initialize(config, Integer.parseInt(schedulerPort));
            }
        }


        if (isDataStore) {
            String[] dataStorePorts = config.getStringArray(DodoorConf.DATA_STORE_THRIFT_PORTS);
            if (dataStorePorts.length == 0 ) {
                dataStorePorts = new String[]{Integer.toString(DodoorConf.DEFAULT_DATA_STORE_THRIFT_PORT)};
            }

            for (String dataStorePort : dataStorePorts) {
                DataStoreThrift dataStore = new DataStoreThrift();
                dataStore.initialize(config, Integer.parseInt(dataStorePort));
            }
        }
    }
}
