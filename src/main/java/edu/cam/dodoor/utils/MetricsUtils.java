package edu.cam.dodoor.utils;

import java.util.Arrays;

public class MetricsUtils {

    public static int getQuantile(int[] values, double quantile) {
        if (values.length == 0) {
            return 0;
        }
        Arrays.sort(values);
        int index = Math.max((int) Math.ceil(quantile * values.length) - 1, 0);
        return values[index];
    }
}
