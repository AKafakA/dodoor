package edu.cam.dodoor.utils;

import edu.cam.dodoor.DodoorConf;

import java.util.Set;

public class SchedulerUtils {

     private static final Set<String> CACHED_SCHEDULERS_SET
             = Set.of(DodoorConf.DODOOR_SCHEDULER, DodoorConf.CACHED_SPARROW_SCHEDULER);

    public static boolean isCachedEnabled(String schedulerType) {
        return CACHED_SCHEDULERS_SET.contains(schedulerType);
    }

    public static boolean isAsyncScheduler(String schedulerType) {
        return schedulerType.startsWith(DodoorConf.ASYNC_SCHEDULER_PREFIX) || schedulerType.equals(DodoorConf.PREQUAL);
    }
}
