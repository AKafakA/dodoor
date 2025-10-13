package edu.cam.dodoor.utils;

import edu.cam.dodoor.DodoorConf;
import org.apache.commons.configuration.Configuration;

import java.util.Set;

public class SchedulerUtils {

     private static final Set<String> CACHED_SCHEDULERS_SET
             = Set.of(DodoorConf.DODOOR_SCHEDULER, DodoorConf.CACHED_POWER_OF_TWO_SCHEDULER);

     // For the scheduler who need to cache the probe results at datastore
    public static boolean isCachedEnabled(String schedulerType) {
        return CACHED_SCHEDULERS_SET.contains(schedulerType);
    }

    // For the scheduler which is async probe but no centralized data store cache needs
    public static boolean isAsyncScheduler(String schedulerType) {
        return schedulerType.equals(DodoorConf.PREQUAL);
    }

    public static int getProbeReuseBudget(int numNode, int probePoolSize, int probeRate, int probeDeleteRate,
                                          int delta) {
        int result = (int) Math.ceil((1 + delta) / ((1 - (double) probePoolSize / numNode) * probeRate - probeDeleteRate));
        return Math.max(result, 1);
    }

    public static boolean isLateBindingScheduler(String schedulerType) {
        return schedulerType.equals(DodoorConf.SPARROW_SCHEDULER);
    }
}
