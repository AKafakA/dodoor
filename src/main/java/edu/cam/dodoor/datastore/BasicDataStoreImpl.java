package edu.cam.dodoor.datastore;

import edu.cam.dodoor.thrift.TNodeState;
import edu.cam.dodoor.utils.ConfigUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Basic implementation for {@link DataStore} to have the node states stored inside memory as List
 */
public class BasicDataStoreImpl implements DataStore{
    private final Map<String, TNodeState> _nodeStates;
    List<InetSocketAddress> _backendAddress;


    public BasicDataStoreImpl(Map<String, TNodeState> nodeStates){
        _nodeStates = nodeStates;
    }

    @Override
    public void initialize(Configuration config) throws TException {
        _backendAddress = new ArrayList<>(ConfigUtil.parseBackends(config));
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
