package edu.cam.dodoor.utils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import com.google.common.base.Optional;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.TResourceVector;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Utilities to aid the configuration file-based scheduler and node monitor.
 */
public class ConfigUtil {
  private final static Logger LOG = Logger.getLogger(ConfigUtil.class);


  public static List<String> parseNodeAddress(
          JSONObject conf, String serviceTypeKey) {

    List<String> nodeAddress = new ArrayList<>();
    JSONObject serviceConfig = conf.getJSONObject(serviceTypeKey);
    if (serviceConfig == null) {
      LOG.warn("No configuration found for " + serviceTypeKey);
      return nodeAddress;
    }
    if (serviceTypeKey.equals(DodoorConf.NODE_SERVICE_NAME)) {
      throw new IllegalArgumentException("Node service not supported in this method. ");
    } else {
      JSONArray hosts = serviceConfig.getJSONArray(DodoorConf.SERVICE_HOST_LIST_KEY);
      JSONArray ports = serviceConfig.getJSONArray(DodoorConf.SERVICE_PORT_LIST_KEY);
      for (int i = 0; i < hosts.length(); i++) {
        String host = hosts.getString(i);
        for (int j = 0; j < ports.length(); j++) {
          String port = String.valueOf(ports.getInt(j));
          String nodePort = host + ":" + port;
          Optional<InetSocketAddress> addr = Serialization.strToSocket(nodePort);
          if (!addr.isPresent()) {
            LOG.warn("Bad backend address: " + nodePort);
          } else {
            nodeAddress.add(nodePort);
          }
        }
      }
    }
    return nodeAddress;
  }
}
