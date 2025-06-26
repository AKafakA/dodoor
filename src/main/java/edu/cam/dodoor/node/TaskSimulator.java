package edu.cam.dodoor.node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * A reusable, runnable task that simulates a specific CPU and memory load for a given duration.
 * This class is designed to be submitted to a thread pool (ExecutorService).
 */
class TaskSimulator {
    private final static Logger LOG = LoggerFactory.getLogger(TaskSimulator.class);

    private final double _targetCpuLoad; // e.g., 0.5 for 50% of one core
    private final int _memoryToAllocateGB;
    private final int _totalDurationMs;
    private byte[] _memoryHog; // Keep a reference to prevent GC

    public TaskSimulator(double targetCpuLoad, int memoryInGB, int durationInMs) {
        if (targetCpuLoad < 0.0 || targetCpuLoad > 1.0) {
            throw new IllegalArgumentException("CPU load must be between 0.0 and 1.0.");
        }
        if (durationInMs <= 0) {
            throw new IllegalArgumentException("Task duration must be positive.");
        }
        _targetCpuLoad = targetCpuLoad;
        _memoryToAllocateGB = memoryInGB;
        _totalDurationMs = durationInMs;
    }

    public void simulate() {
        try {
            // 1. Allocate and hold memory (if requested)
            if (_memoryToAllocateGB > 0) {
                long bytesToAllocate = (long) _memoryToAllocateGB * 1024 * 1024 * 1024;
                // Java array size is limited to Integer.MAX_VALUE
                if (bytesToAllocate > Integer.MAX_VALUE - 8) {
                    throw new OutOfMemoryError("Requested memory exceeds max Java array size.");
                }
                // This loop "touches" memory pages, ensuring the OS actually commits the memory
                // and preventing the JIT compiler from optimizing away the allocation.
                for (int i = 0; i < _memoryHog.length; i += 4096) {
                    _memoryHog[i] = (byte) (i & 0xFF);
                }
                _memoryHog = new byte[(int) bytesToAllocate];
            }

            // 2. Perform timed CPU work/sleep cycles for the total duration
            // **KEY CHANGE**: Use a small, 1ms cycle for high fidelity across all task durations.
            final long cycleDurationNs = TimeUnit.MILLISECONDS.toNanos(1);
            final long cpuActiveNs = (long) (cycleDurationNs * _targetCpuLoad);
            final long simulationEndTimeNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(_totalDurationMs);

            while (System.nanoTime() < simulationEndTimeNs) {
                final long cycleStartTimeNs = System.nanoTime();
                int randomNumber = 0;
                // Busy-wait to consume CPU
                while ((System.nanoTime() - cycleStartTimeNs) < cpuActiveNs) {
                    // This loop burns CPU cycles. It's intentionally empty.
                    randomNumber = Math.random() > 0.5 ? 1 : 0; // Dummy operation to prevent optimization
                }
                // Sleep for the remainder of the 1ms cycle
                long sleepTimeNs = cycleDurationNs - (System.nanoTime() - cycleStartTimeNs);
                if (sleepTimeNs > 0) {
                    TimeUnit.NANOSECONDS.sleep(sleepTimeNs);
                }
            }

        } catch (OutOfMemoryError e) {
            LOG.error("TaskSimulator encountered an OutOfMemoryError: {}", e.getMessage());
            // Handle memory allocation failure gracefully
        } catch (InterruptedException e) {
            LOG.warn("TaskSimulator was interrupted: {}", e.getMessage());
            // Restore the interrupted status
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.error("TaskSimulator encountered an unexpected error: {}", e.getMessage());
        } finally {
            // Release memory reference to allow garbage collection
            _memoryHog = null;
            LOG.info(String.format("TaskSimulator completed: CPU Load=%.2f, Memory Allocated=%dGB, Duration=%dms",
                    _targetCpuLoad, _memoryToAllocateGB, _totalDurationMs));
        }
    }
}