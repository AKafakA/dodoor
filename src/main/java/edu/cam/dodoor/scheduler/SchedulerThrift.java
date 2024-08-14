package edu.cam.dodoor.scheduler;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Slf4jReporter;
import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.*;
import edu.cam.dodoor.utils.Network;
import edu.cam.dodoor.utils.TServers;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.log4j.FileAppender;
import org.apache.log4j.PatternLayout;
import org.apache.thrift.TException;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class SchedulerThrift implements SchedulerService.Iface{
    private Scheduler _scheduler;
    private Counter _numMessages;
    private Counter _numFinishedTasks;


    @Override
    public void submitJob(TSchedulingRequest req) throws TException {
        _numMessages.inc();
        _scheduler.submitJob(req);
    }

    @Override
    public void updateNodeState(Map<String, TNodeState> snapshot) {
        _numMessages.inc();
        _scheduler.updateNodeState(snapshot);
    }

    @Override
    public void registerNode(String nodeAddress) throws TException {
        _scheduler.registerNode(nodeAddress);
    }

    @Override
    public void registerDataStore(String dataStoreAddress) throws TException {
        _scheduler.registerDataStore(dataStoreAddress);
    }

    @Override
    public void taskFinished(TFullTaskId task) throws TException {
        _numFinishedTasks.inc();
        _scheduler.taskFinished(task);
    }

    public void initialize(Configuration config, int port, boolean logKicked) throws TException, IOException {
        _scheduler = new SchedulerImpl();
        SchedulerService.Processor<SchedulerService.Iface> processor =
                new SchedulerService.Processor<>(this);
        int threads = config.getInt(DodoorConf.SCHEDULER_THRIFT_THREADS,
                DodoorConf.DEFAULT_SCHEDULER_THRIFT_THREADS);
        MetricRegistry metrics = SharedMetricRegistries.getOrCreate(DodoorConf.SCHEDULER_METRICS_REGISTRY);
        SchedulerServiceMetrics schedulerMetrics = new SchedulerServiceMetrics(metrics);
        _numMessages = schedulerMetrics.getTotalMessages();
        _numFinishedTasks = metrics.counter("num.finished.tasks");
        _scheduler.initialize(config, Network.getInternalHostPort(port, config), schedulerMetrics);
        TServers.launchThreadedThriftServer(port, threads, processor);

        // Avoid one log kicked duplicated from different scheduler instances
        if (config.getBoolean(DodoorConf.TRACKING_ENABLED, DodoorConf.DEFAULT_TRACKING_ENABLED) && !logKicked) {
            String schedulerLogPathSuffix = config.getString(DodoorConf.SCHEDULER_METRICS_LOG_FILE_SUFFIX,
                    DodoorConf.DEFAULT_SCHEDULER_METRICS_LOG_FILE_SUFFIX);
            String schedulerLogPath = config.getString(DodoorConf.SCHEDULER_TYPE, DodoorConf.DODOOR_SCHEDULER)
                    + "_" +schedulerLogPathSuffix;
            org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(SchedulerThrift.class);
            logger.setAdditivity(false);
            try {
                logger.addAppender(new FileAppender(new PatternLayout(), schedulerLogPath));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            final Slf4jReporter reporter = Slf4jReporter.forRegistry(metrics)
                    .outputTo(LoggerFactory.getLogger(SchedulerThrift.class))
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();
            reporter.start(config.getInt(DodoorConf.TRACKING_INTERVAL_IN_SECONDS, DodoorConf.DEFAULT_TRACKING_INTERVAL),
                    TimeUnit.SECONDS);
        }
    }
}
