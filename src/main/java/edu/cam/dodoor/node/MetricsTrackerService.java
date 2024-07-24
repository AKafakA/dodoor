package edu.cam.dodoor.node;

import com.codahale.metrics.Slf4jReporter;
import edu.cam.dodoor.DodoorConf;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.*;

import java.io.File;
import com.sun.management.OperatingSystemMXBean;
import org.slf4j.LoggerFactory;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.HardwareAbstractionLayer;

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

    private final CentralProcessor _cpu;
    private static final long SAMPLED_INTERVAL = 10;
    private long[] _prevTicks;


    public MetricsTrackerService(int trackingInterval, Configuration config, NodeServiceMetrics nodeServiceMetrics) {
        _operatingSystemMXBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        _trackingInterval = trackingInterval;
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
        SystemInfo _si = new SystemInfo();
        HardwareAbstractionLayer _hal = _si.getHardware();
        _cpu = _hal.getProcessor();
        _prevTicks = _cpu.getSystemCpuLoadTicks();

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
                } catch (InterruptedException e) {
                    LOG.error("Metrics tracker thread interrupted", e);
                }
            }
        }
    }

    private void logUsage() throws InterruptedException {
//        double cpuUsage = _operatingSystemMXBean.getCpuLoad();
        long[] prevTicks = _cpu.getSystemCpuLoadTicks();
        // 睡眠1s
        TimeUnit.SECONDS.sleep(1);
        long[] ticks = _cpu.getSystemCpuLoadTicks();
        long nice = ticks[CentralProcessor.TickType.NICE.getIndex()] - prevTicks[CentralProcessor.TickType.NICE.getIndex()];
        long irq = ticks[CentralProcessor.TickType.IRQ.getIndex()] - prevTicks[CentralProcessor.TickType.IRQ.getIndex()];
        long softirq = ticks[CentralProcessor.TickType.SOFTIRQ.getIndex()] - prevTicks[CentralProcessor.TickType.SOFTIRQ.getIndex()];
        long steal = ticks[CentralProcessor.TickType.STEAL.getIndex()] - prevTicks[CentralProcessor.TickType.STEAL.getIndex()];
        long cSys = ticks[CentralProcessor.TickType.SYSTEM.getIndex()] - prevTicks[CentralProcessor.TickType.SYSTEM.getIndex()];
        long user = ticks[CentralProcessor.TickType.USER.getIndex()] - prevTicks[CentralProcessor.TickType.USER.getIndex()];
        long iowait = ticks[CentralProcessor.TickType.IOWAIT.getIndex()] - prevTicks[CentralProcessor.TickType.IOWAIT.getIndex()];
        long idle = ticks[CentralProcessor.TickType.IDLE.getIndex()] - prevTicks[CentralProcessor.TickType.IDLE.getIndex()];
        long totalCpu = user + nice + cSys + idle + iowait + irq + softirq + steal;
        double cpuUsage = 1.0-(idle * 1.0 / totalCpu);
        double memoryUsage =
                (double) (_systemMemory - _operatingSystemMXBean.getFreeMemorySize()) / _systemMemory;
        double freeSpace =  (double) _root.getFreeSpace() / _totalSpace;
        LOG.info("Time(in Seconds): {} CPU usage: {} Memory usage: {} Disk usage: {}", new Object[]{_timelineInSeconds, cpuUsage, memoryUsage, freeSpace});
        _prevTicks = _cpu.getSystemCpuLoadTicks();
    }

    public void start() {
        Thread t = new Thread(new MetricTrackRunnable());
        t.start();
        _slf4jReporter.start(_trackingInterval, TimeUnit.SECONDS);
    }
}
