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
        ServiceDaemon daemon = new ServiceDaemon();
        daemon.initialize(conf);
    }

    private void initialize(Configuration config) throws Exception{
        Level logLevel = Level.toLevel(config.getString(DodoorConf.LOG_LEVEL, ""),
                DEFAULT_LOG_LEVEL);
        Logger.getRootLogger().setLevel(logLevel);

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

        String[] schedulerPorts = config.getStringArray(DodoorConf.SCHEDULER_THRIFT_PORTS);

        if (schedulerPorts.length == 0) {
            schedulerPorts = new String[]{Integer.toString(DodoorConf.DEFAULT_SCHEDULER_THRIFT_PORT)};
        }

        for (String schedulerPort : schedulerPorts) {
            SchedulerThrift scheduler = new SchedulerThrift();
            scheduler.initialize(config, Integer.parseInt(schedulerPort));
        }

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
