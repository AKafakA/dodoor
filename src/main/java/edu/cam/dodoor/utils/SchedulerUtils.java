package edu.cam.dodoor.utils;

import edu.cam.dodoor.DodoorConf;

import java.util.HashSet;
import java.util.Set;

public class SchedulerUtils {

     private static final Set<String> CACHED_SCHEDULERS_SET
             = Set.of(DodoorConf.DODOOR_SCHEDULER, DodoorConf.CACHED_SPARROW_SCHEDULER, DodoorConf.REVERSE_DODOOR_SCHEDULER);

    public static boolean isCachedEnabled(String schedulerType) {
        return CACHED_SCHEDULERS_SET.contains(schedulerType);
    }
}
