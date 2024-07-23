package edu.cam.dodoor.utils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.commons.configuration.Configuration;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.THostPort;

public class Network {
  
  public static THostPort socketAddressToThrift(InetSocketAddress address) {
    return new THostPort(address.getAddress().getHostAddress(), address.getPort());
  }

  /** Return the hostname of this machine, based on configured value, or system
   * Interrogation. */
  public static String getHostName(Configuration conf) {
    String defaultHostname = null;
    try {
      defaultHostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      defaultHostname = "localhost";
    }
    return conf.getString(DodoorConf.HOSTNAME, defaultHostname);
  }
  
  /**
   * Return the IP address of this machine, as determined from the hostname
   * specified in configuration or from querying the machine.
   */
  public static String getLocalIPAddress(Configuration conf) {
    String hostname = getHostName(conf);
    try {
      return InetAddress.getByName(hostname).getHostAddress();
    } catch (UnknownHostException e) {
      return "IP UNKNOWN";
    }
  }
}
