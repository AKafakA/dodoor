package edu.cam.dodoor.datastore;

import edu.cam.dodoor.thrift.TNodeState;
import edu.cam.dodoor.thrift.TResourceVector;
import org.apache.commons.configuration.Configuration;

import java.util.Map;

public interface DataStore {

    void initialize(Configuration config);

    void updateNodeLoad(String nodeEnqueueAddress, TNodeState nodeStates);

    void addNodeLoad(String nodeEnqueueAddress, TResourceVector resourceVector, int numTasks, int sign);

    Map<String, TNodeState> getNodeStates();

    boolean containsNode(String nodeEnqueueAddress);
}
