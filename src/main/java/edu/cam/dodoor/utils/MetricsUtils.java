package edu.cam.dodoor.utils;

import java.util.Arrays;

public class MetricsUtils {

    public static int getQuantile(int[] values, double quantile) {
        if (values.length == 0) {
            return 0;
        }
        Arrays.sort(values);
        int index = (int) Math.ceil(quantile * values.length);
        return values[index];
    }
}
