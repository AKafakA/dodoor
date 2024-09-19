package edu.cam.dodoor.datastore;

import edu.cam.dodoor.thrift.TNodeState;
import edu.cam.dodoor.thrift.TResourceVector;
import org.apache.commons.configuration.Configuration;

import java.util.Map;

public interface DataStore {

    void initialize(Configuration config);

    void overrideNodeLoad(String nodeEnqueueAddress, TNodeState nodeStates);

    void addNodeLoads(Map<String, TNodeState> additionalNodeLoad, int sign);

    Map<String, TNodeState> getNodeStates();

    boolean containsNode(String nodeEnqueueAddress);
}
