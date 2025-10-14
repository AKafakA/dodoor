package org.anon.scheduler.datastore;

import org.anon.scheduler.thrift.TNodeState;
import org.anon.scheduler.thrift.TResourceVector;
import org.apache.commons.configuration.Configuration;

import java.util.Map;

public interface DataStore {

    void initialize(Configuration config);

    void overrideNodeLoad(String nodeEnqueueAddress, TNodeState nodeStates);

    void addNodeLoads(Map<String, TNodeState> additionalNodeLoad, int sign);

    Map<String, TNodeState> getNodeStates();

    boolean containsNode(String nodeEnqueueAddress);
}
