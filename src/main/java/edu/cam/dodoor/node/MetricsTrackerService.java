package edu.cam.dodoor.node;

import com.codahale.metrics.Slf4jReporter;
import edu.cam.dodoor.DodoorConf;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.*;

import java.io.File;
import com.sun.management.OperatingSystemMXBean;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;


public class MetricsTrackerService {

    private final org.slf4j.Logger LOG;

    private final int _trackingInterval;
    private final File _root;
    private final double _totalSpace;
    private final OperatingSystemMXBean _operatingSystemMXBean;
    private final long _systemMemory;
    private long _timelineInSeconds;
    private final Slf4jReporter _slf4jReporter;
    private final TaskLauncherService _taskLauncherService;


    public MetricsTrackerService(int trackingInterval, Configuration config, NodeServiceMetrics nodeServiceMetrics,
                                 TaskLauncherService taskLauncherService) {
        _operatingSystemMXBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        _trackingInterval = trackingInterval;
        _taskLauncherService = taskLauncherService;
        _root = new File("/");
        _totalSpace = _root.getTotalSpace();
        _systemMemory = _operatingSystemMXBean.getTotalMemorySize();
        String tracingFile = config.getString(DodoorConf.NODE_METRICS_LOG_FILE, DodoorConf.DEFAULT_NODE_METRICS_LOG_FILE);
        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(MetricsTrackerService.class);
        logger.setAdditivity(false);
        try {
            logger.addAppender(new FileAppender(new PatternLayout(), tracingFile));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        _timelineInSeconds = 0;

        LOG = LoggerFactory.getLogger(MetricsTrackerService.class);

        _slf4jReporter = Slf4jReporter.forRegistry(nodeServiceMetrics._metrics)
                .outputTo(LOG)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
    }

    private class MetricTrackRunnable implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(_trackingInterval));
                    _timelineInSeconds += _trackingInterval;
                    logUsage();
                    logLaunchService();
                } catch (InterruptedException e) {
                    LOG.error("Metrics tracker thread interrupted", e);
                }
            }
        }
    }

    private void logUsage() throws InterruptedException {
        double cpuUsage = _operatingSystemMXBean.getCpuLoad();
        double memoryUsage =
                (double) (_systemMemory - _operatingSystemMXBean.getFreeMemorySize()) / _systemMemory;
        double diskUsage =  (double) _root.getFreeSpace() / _totalSpace;
        LOG.info("Time(in Seconds) OSM: {} CPU usage: {} Memory usage: {} Disk usage: {}", new Object[]{_timelineInSeconds, cpuUsage, memoryUsage, diskUsage});
    }

    private void logLaunchService() {
        int numActivateTasks = _taskLauncherService.getActiveTasks();
        long numTotalTasks = _taskLauncherService.getTaskCount();
        long numCompletedTasks = _taskLauncherService.getCompletedTaskCount();
        LOG.info("Time(in Seconds) TaskLauncherService: {} Active tasks: {} Total tasks: {} Completed tasks: {}",
                new Object[]{_timelineInSeconds, numActivateTasks, numTotalTasks, numCompletedTasks}
        );
    }

    public void start() {
        Thread t = new Thread(new MetricTrackRunnable());
        t.start();
        _slf4jReporter.start(_trackingInterval, TimeUnit.SECONDS);
    }
}
