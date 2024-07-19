package edu.cam.dodoor.node;

import edu.cam.dodoor.DodoorConf;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.*;

import java.io.File;
import com.sun.management.OperatingSystemMXBean;

import java.io.IOException;
import java.lang.management.ManagementFactory;

public class MetricsTrackerService {

    private final Logger LOG;

    private final int _trackingInterval;
    private final File _root;
    private final double _totalSpace;
    private final OperatingSystemMXBean _operatingSystemMXBean;
    private final long _systemMemory;
    private long _timeline;

    public MetricsTrackerService(int trackingInterval, Configuration config) {
        _operatingSystemMXBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        _trackingInterval = trackingInterval;
        _root = new File("/");
        _totalSpace = _root.getTotalSpace();
        _systemMemory = _operatingSystemMXBean.getTotalPhysicalMemorySize();
        String _tracingFile = config.getString(DodoorConf.METRICS_LOG_FILE, DodoorConf.DEFAULT_METRICS_LOG_FILE);

        LOG = Logger.getLogger(MetricsTrackerService.class);
        LOG.setAdditivity(false);
        try {
            LOG.addAppender(new FileAppender(new PatternLayout(), _tracingFile));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        _timeline = 0;
    }

    private class MetricTrackRunnable implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(_trackingInterval);
                    _timeline += _trackingInterval;
                    logUsage();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void logUsage() {
        double cpuUsage = _operatingSystemMXBean.getSystemCpuLoad();
        double memoryUsage =
                (double) (_systemMemory - _operatingSystemMXBean.getFreePhysicalMemorySize()) / _systemMemory;
        double freeSpace =  (double) _root.getFreeSpace() / _totalSpace;
        LOG.info("Time(MS): " +
                _timeline + " CPU usage: " + cpuUsage + " Memory usage: " + memoryUsage + " Disk usage: " + freeSpace);
    }

    public void start() {
        Thread t = new Thread(new MetricTrackRunnable());
        t.start();
    }
}
