package edu.cam.dodoor.utils;

import java.net.*;
import java.util.Enumeration;

import org.apache.commons.configuration.Configuration;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.thrift.THostPort;

public class Network {
  
  public static THostPort socketAddressToThrift(InetSocketAddress address) {
    return new THostPort(address.getAddress().getHostAddress(), address.getPort());
  }

  public static InetSocketAddress thriftToSocket(THostPort address) {
    return new InetSocketAddress(address.host, address.port);
  }

  public static String thriftToSocketStr(THostPort address) {
    return address.port + ":" + address.host;
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

  public static THostPort getInternalHostPort(int port, Configuration config) throws SocketException {
    Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
    String networkInterfaceName = config.getString(DodoorConf.NETWORK_INTERFACE, DodoorConf.DEFAULT_NETWORK_INTERFACE);
    while (interfaces.hasMoreElements()) {
      NetworkInterface networkInterface = interfaces.nextElement();
      if (!networkInterface.getName().equals(networkInterfaceName)) {
        continue;
      }
      Enumeration<java.net.InetAddress> addresses = networkInterface.getInetAddresses();
      while (addresses.hasMoreElements()) {
        java.net.InetAddress address = addresses.nextElement();
        if (address.isLoopbackAddress()) {
          continue;
        }
        if (address.isSiteLocalAddress()) {
          return new THostPort(address.getHostAddress(), port);
        }
      }
    }
    return new THostPort(Network.getLocalIPAddress(config), port);
  }
}
