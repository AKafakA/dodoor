package edu.cam.dodoor.datastore;

import edu.cam.dodoor.thrift.TNodeState;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import java.util.Map;

/**
 * Basic implementation for {@link DataStore} to have the node states stored inside memory as List
 */
public class BasicDataStoreImpl implements DataStore{

    private static final Logger LOG = Logger.getLogger(BasicDataStoreImpl.class);
    private final Map<String, TNodeState> _nodeStates;


    public BasicDataStoreImpl(Map<String, TNodeState> nodeStates){
        _nodeStates = nodeStates;
    }

    @Override
    public void initialize(Configuration config) {
    }

    @Override
    public void updateNodeLoad(String nodeEnqueueAddress, TNodeState nodeStates) {
        LOG.debug("Updating node load for " + nodeEnqueueAddress);
        _nodeStates.put(nodeEnqueueAddress, nodeStates);
    }

    @Override
    public Map<String, TNodeState> getNodeStates() {
        return _nodeStates;
    }

    @Override
    public boolean containsNode(String nodeEnqueueAddress) {
        return _nodeStates.containsKey(nodeEnqueueAddress);
    }

}
