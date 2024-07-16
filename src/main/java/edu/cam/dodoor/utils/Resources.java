package edu.cam.dodoor.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.configuration.Configuration;

import edu.cam.dodoor.DodoorConf;

/** Utilities for interrogating system resources. */
public class Resources {
  public static int getSystemMemoryMb(Configuration conf) {
    int systemMemory = -1;
    try {
      Process p = Runtime.getRuntime().exec("cat /proc/meminfo");  
      BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line = in.readLine();
      while (line != null) {
        if (line.contains("MemTotal")) { 
          String[] parts = line.split("\\s+");
          if (parts.length > 1) {
            int memory = Integer.parseInt(parts[1]) / 1000;
            systemMemory = memory;
          }
        }
        line = in.readLine();
      }
    } catch (IOException ignored) {}
    if (conf.containsKey(DodoorConf.SYSTEM_MEMORY)) {
      return conf.getInt(DodoorConf.SYSTEM_MEMORY);
    } else {
      if (systemMemory != -1) {
        return systemMemory;
      } else {
        return DodoorConf.DEFAULT_SYSTEM_MEMORY;
      }
    }
  }
  
  public static double getSystemCPUCount(Configuration conf) {
    // No system interrogation yet
    return conf.getInt(DodoorConf.SYSTEM_CPUS, DodoorConf.DEFAULT_SYSTEM_CPUS);
  }

  public static int getSystemDiskGb(Configuration conf) {
    return conf.getInt(DodoorConf.SYSTEM_DISK, DodoorConf.DEFAULT_SYSTEM_DISK);
  }
}
