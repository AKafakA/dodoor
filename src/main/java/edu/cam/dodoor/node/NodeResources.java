package edu.cam.dodoor.node;

import com.google.common.util.concurrent.AtomicDouble;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class NodeResources {
    private static final Logger LOG = LoggerFactory.getLogger(NodeResources.class);
    private final AtomicDouble _cores;
    private final AtomicLong _memory;
    private final AtomicLong _disk;

    public NodeResources(double cores, long memory, long disk) {
        _cores = new AtomicDouble(cores);
        _memory = new AtomicLong(memory);
        _disk = new AtomicLong(disk);
    }

    public synchronized boolean runTaskIfPossible(double cores, long memory, long disk) {
        LOG.debug("Current resources: cores={}, memory={}, disk={} and requested resources {}, {}, {}",
                new Object[]{_cores.get(), _memory.get(), _disk.get(), cores, memory, disk});
        boolean canRun = _cores.get() >= cores && _memory.get() >= memory && _disk.get() >= disk;
        if (canRun) {
            _cores.addAndGet(-cores);
            _memory.addAndGet(-memory);
            _disk.addAndGet(-disk);
            return true;
        }
        return false;
    }

    public synchronized boolean enoughToRun(double cores, long memory, long disk) {
        return _cores.get() >= cores && _memory.get() >= memory && _disk.get() >= disk;
    }

    public synchronized void freeTask(double cores, long memory, long disk) {
        _cores.addAndGet(cores);
        _memory.addAndGet(memory);
        _disk.addAndGet(disk);
    }
}
