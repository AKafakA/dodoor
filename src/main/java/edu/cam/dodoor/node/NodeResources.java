package edu.cam.dodoor.node;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class NodeResources {
    private final AtomicInteger _cores;
    private final AtomicLong _memory;
    private final AtomicLong _disk;

    public NodeResources(int cores, long memory, long disk) {
        _cores = new AtomicInteger(cores);
        _memory = new AtomicLong(memory);
        _disk = new AtomicLong(disk);
    }

    public synchronized boolean runTaskIfPossible(int cores, long memory, long disk) {
        boolean canRun = _cores.get() >= cores && _memory.get() >= memory && _disk.get() >= disk;
        if (canRun) {
            _cores.addAndGet(-cores);
            _memory.addAndGet(-memory);
            _disk.addAndGet(-disk);
            return true;
        }
        return false;
    }

    public synchronized void freeTask(int cores, long memory, long disk) {
        _cores.addAndGet(cores);
        _memory.addAndGet(memory);
        _disk.addAndGet(disk);
    }
}
