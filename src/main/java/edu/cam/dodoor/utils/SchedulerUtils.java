package edu.cam.dodoor.utils;

import edu.cam.dodoor.DodoorConf;

public class SchedulerUtils {

    public static boolean isCachedEnabled(String schedulerType) {
        return !schedulerType.equals(DodoorConf.SPARROW_SCHEDULER);
    }
}
