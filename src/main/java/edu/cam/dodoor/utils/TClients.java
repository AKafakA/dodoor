package edu.cam.dodoor.utils;

import java.io.IOException;
import java.net.InetSocketAddress;

import edu.cam.dodoor.thrift.*;
import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Helper functions for creating Thrift clients for various Sparrow interfaces.
 */
public class TClients {
  private final static Logger LOG = Logger.getLogger(TClients.class);

  public static SchedulerService.Client createBlockingSchedulerClient(
      InetSocketAddress socket) throws IOException, TTransportException {
    return createBlockingSchedulerClient(socket.getAddress().getHostAddress(), socket.getPort());
  }

  public static SchedulerService.Client createBlockingSchedulerClient(
      String host, int port) throws IOException, TTransportException {
    return createBlockingSchedulerClient(host, port, 0);
  }

  public static SchedulerService.Client createBlockingSchedulerClient(
      String host, int port, int timeout) throws IOException, TTransportException {
    return new SchedulerService.Client(getProtocol(host, port, timeout));
  }

  private static TBinaryProtocol getProtocol(String host, int port, int timeout) throws TTransportException, IOException {
    TTransport tr = new TFramedTransport(new TSocket(host, port, timeout));
    try {
      tr.open();
    } catch (TTransportException e) {
      LOG.warn("Error creating scheduler client to " + host + ":" + port);
      throw new IOException(e);
    }
    return new TBinaryProtocol(tr);
  }
}
