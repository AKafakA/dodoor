package edu.cam.dodoor.nodemonitor;

import org.apache.log4j.Logger;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

public class MetricsTrackerService {

    private final Logger LOG = Logger.getLogger(MetricsTrackerService.class);

    private final int _trackingInterval;
    private final File _root;
    private final double _totalSpace;
    private static final int GB_SIZE = 1073741824;

    public MetricsTrackerService(int trackingInterval) {
        _trackingInterval = trackingInterval;
        _root = new File("/");
        _totalSpace = (double) _root.getTotalSpace() / GB_SIZE;
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
        OperatingSystemMXBean operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean();
        for (Method method : operatingSystemMXBean.getClass().getDeclaredMethods()) {
            method.setAccessible(true);
            if (method.getName().startsWith("get")
                    && Modifier.isPublic(method.getModifiers())) {
                Object value;
                try {
                    value = method.invoke(operatingSystemMXBean);
                } catch (Exception e) {
                    value = e;
                }
                LOG.trace(method.getName() + " = " + value);
            }
        }
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
