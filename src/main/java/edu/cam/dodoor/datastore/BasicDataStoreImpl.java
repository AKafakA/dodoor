package edu.cam.dodoor.datastore;

import edu.cam.dodoor.thrift.TNodeState;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;

import java.util.Map;

/**
 * Basic implementation for {@link DataStore} to have the node states stored inside memory as List
 */
public class BasicDataStoreImpl implements DataStore{
    private final Map<String, TNodeState> _nodeStates;


    public BasicDataStoreImpl(Map<String, TNodeState> nodeStates){
        _nodeStates = nodeStates;
    }

    @Override
    public void initialize(Configuration config) throws TException {
    }

    @Override
    public void updateNodeLoad(String nodeMonitorAddress, TNodeState nodeStates) throws TException {
        _nodeStates.put(nodeMonitorAddress, nodeStates);
    }

    @Override
    public Map<String, TNodeState> getNodeStates() throws TException {
        return _nodeStates;
    }
}
