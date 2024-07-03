package edu.cam.dodoor.scheduler;

import edu.cam.dodoor.utils.Logging;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

public class Scheduler {
    private final static Logger LOG = Logger.getLogger(Scheduler.class);
    private final static Logger AUDIT_LOG = Logging.getAuditLogger(Scheduler.class);

    /** Used to uniquely identify requests arriving at this scheduler. */
    private AtomicInteger counter = new AtomicInteger(0);

}
