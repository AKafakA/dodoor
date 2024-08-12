package edu.cam.dodoor.node;

import com.codahale.metrics.Slf4jReporter;
import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.utils.Resources;
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
    private final long _memoryCapacity;


    public MetricsTrackerService(int trackingInterval, Configuration config, NodeServiceMetrics nodeServiceMetrics,
                                 TaskLauncherService taskLauncherService) {
        _operatingSystemMXBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        _trackingInterval = trackingInterval;
        _taskLauncherService = taskLauncherService;
        _root = new File("/");
        _totalSpace = _root.getTotalSpace();
        _systemMemory = _operatingSystemMXBean.getTotalMemorySize();
        // track how much memory is occupied before the task is launched so that we can calculate the memory usage by the tasks only later
        // The configured memory allowed to be scheduled for tasks should be lower than the actual system memory to avoid OOM
        _memoryCapacity = Resources.getMemoryMbCapacity(config);
        String tracingFileSuffix = config.getString(DodoorConf.NODE_METRICS_LOG_FILE_SUFFIX, DodoorConf.DEFAULT_NODE_METRICS_LOG_FILE_SUFFIX);
        String tracingFile = config.getString(DodoorConf.SCHEDULER_TYPE, DodoorConf.DODOOR_SCHEDULER) + "_" + tracingFileSuffix;
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
        double usedMemory = _systemMemory - _operatingSystemMXBean.getFreeMemorySize();
        double memoryUsage = usedMemory / _systemMemory;
        double diskUsage =  (double) _root.getFreeSpace() / _totalSpace;
        LOG.info("Time(in Seconds) OSM: {} CPU usage: {} Memory usage: {} Disk usage: {}", new Object[]{_timelineInSeconds, cpuUsage, memoryUsage, diskUsage});
    }

    private void logLaunchService() {
        int numActivateTasks = _taskLauncherService.getActiveTasks();
        long numTotalTasks = _taskLauncherService.getTaskCount();
        long numCompletedTasks = _taskLauncherService.getCompletedTaskCount();
        long numQueuedTasks = _taskLauncherService.getQueuedTaskCount();
        LOG.debug("Time(in Seconds) ThreadPool: {} Active tasks: {} Total tasks: {} " +
                        "Completed tasks: {} Queued tasks: {}",
                new Object[]{_timelineInSeconds, numActivateTasks, numTotalTasks, numCompletedTasks, numQueuedTasks});
    }

    public void start() {
        Thread t = new Thread(new MetricTrackRunnable());
        t.start();
        _slf4jReporter.start(_trackingInterval, TimeUnit.SECONDS);
    }
}
