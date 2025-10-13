package edu.cam.dodoor.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import edu.cam.dodoor.thrift.TResourceVector;
import org.apache.commons.configuration.Configuration;

import edu.cam.dodoor.DodoorConf;
import org.json.JSONObject;

/** Utilities for interrogating system resources. */
public class Resources {
  public static int getMemoryMbCapacity(JSONObject nodeConfig) {
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
    if (nodeConfig.has(DodoorConf.SYSTEM_MEMORY)) {
      return nodeConfig.getInt(DodoorConf.SYSTEM_MEMORY);
    } else {
      if (systemMemory != -1) {
        return systemMemory;
      } else {
        return DodoorConf.DEFAULT_SYSTEM_MEMORY;
      }
    }
  }
  
  public static int getSystemCoresCapacity(JSONObject nodeConfig) {
    // No system interrogation yet
    return nodeConfig.optInt(DodoorConf.SYSTEM_CORES, DodoorConf.DEFAULT_SYSTEM_CORES);
  }

  public static int getSystemDiskGbCapacity(JSONObject nodeConfig) {
    return nodeConfig.optInt(DodoorConf.SYSTEM_DISK, DodoorConf.DEFAULT_SYSTEM_DISK);
  }

  public static TResourceVector getSystemResourceVector(Configuration staticConf, JSONObject nodeConfig) {
    if (staticConf.getBoolean(DodoorConf.REPLAY_WITH_DISK, DodoorConf.DEFAULT_REPLAY_WITH_DISK)) {
      return new TResourceVector(getSystemCoresCapacity(nodeConfig), getMemoryMbCapacity(nodeConfig),
              getSystemDiskGbCapacity(nodeConfig));
    } else {
        return new TResourceVector(getSystemCoresCapacity(nodeConfig), getMemoryMbCapacity(nodeConfig), 0);
    }
  }
}
