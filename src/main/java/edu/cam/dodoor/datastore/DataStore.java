package edu.cam.dodoor.datastore;

import edu.cam.dodoor.thrift.TNodeState;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;

import java.util.Map;

public interface DataStore {

    void initialize(Configuration config);

    void updateNodeLoad(String nodeMonitorAddress, TNodeState nodeStates);

    Map<String, TNodeState> getNodeStates();

    boolean containsNode(String nodeMonitorAddress);
}
