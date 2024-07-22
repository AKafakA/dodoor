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

    private final Logger LOG;

    private final int _trackingInterval;
    private final File _root;
    private final double _totalSpace;
    private final OperatingSystemMXBean _operatingSystemMXBean;
    private final long _systemMemory;
    private long _timelineInSeconds;
    private Slf4jReporter _slf4jReporter;

    public MetricsTrackerService(int trackingInterval, Configuration config, NodeServiceMetrics nodeServiceMetrics) {
        _operatingSystemMXBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        _trackingInterval = trackingInterval;
        _root = new File("/");
        _totalSpace = _root.getTotalSpace();
        _systemMemory = _operatingSystemMXBean.getTotalMemorySize();
        String _tracingFile = config.getString(DodoorConf.METRICS_LOG_FILE, DodoorConf.DEFAULT_METRICS_LOG_FILE);

        LOG = Logger.getLogger(MetricsTrackerService.class);
        LOG.setAdditivity(false);
        try {
            LOG.addAppender(new FileAppender(new PatternLayout(), _tracingFile));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        _timelineInSeconds = 0;

        _slf4jReporter = Slf4jReporter.forRegistry(nodeServiceMetrics._metrics)
                .outputTo(LoggerFactory.getLogger(MetricsTrackerService.class))
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
                } catch (InterruptedException e) {
                    LOG.error("Metrics tracker thread interrupted", e);
                }
            }
        }
    }

    private void logUsage() {
        double cpuUsage = _operatingSystemMXBean.getCpuLoad();
        double memoryUsage =
                (double) (_systemMemory - _operatingSystemMXBean.getFreeMemorySize()) / _systemMemory;
        double freeSpace =  (double) _root.getFreeSpace() / _totalSpace;
        LOG.info("Time(in Seconds): " +
                _timelineInSeconds + " CPU usage: " + cpuUsage + " Memory usage: " + memoryUsage + " Disk usage: " + freeSpace);
    }

    public void start() {
        Thread t = new Thread(new MetricTrackRunnable());
        t.start();
        _slf4jReporter.start(_trackingInterval, TimeUnit.SECONDS);
    }
}
