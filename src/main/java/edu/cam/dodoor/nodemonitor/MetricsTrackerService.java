package edu.cam.dodoor.nodemonitor;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import java.io.File;
import com.sun.management.OperatingSystemMXBean;

import java.lang.management.ManagementFactory;

public class MetricsTrackerService {

    private final Logger LOG = Logger.getLogger(MetricsTrackerService.class);

    private final int _trackingInterval;
    private final File _root;
    private final double _totalSpace;
    private static final int GB_SIZE = 1073741824;
    private final OperatingSystemMXBean _operatingSystemMXBean;
    private final long _systemMemory;

    public MetricsTrackerService(int trackingInterval, Configuration config) {
        _operatingSystemMXBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        _trackingInterval = trackingInterval;
        _root = new File("/");
        _totalSpace = (double) _root.getTotalSpace() / GB_SIZE;
        _systemMemory = _operatingSystemMXBean.getTotalPhysicalMemorySize();
    }

    private class MetricTrackRunnable implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    Thread.sleep(_trackingInterval);
                    logUsage();
                    logDisk();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void logUsage() {
        double cpuUsage = _operatingSystemMXBean.getSystemCpuLoad();
        LOG.trace("CPU Usage = " + cpuUsage);
        double memoryUsage =
                (double) (_systemMemory - _operatingSystemMXBean.getFreePhysicalMemorySize()) / _systemMemory;
        LOG.trace("Memory Usage = " + memoryUsage);
    }

    private void logDisk() {
        double freeSpace =  (double) _root.getFreeSpace() / GB_SIZE;
        LOG.trace("Disk Usage =" + freeSpace / _totalSpace);
    }

    public void start() {
        Thread t = new Thread(new MetricTrackRunnable());
        t.start();
    }
}
