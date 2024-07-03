package edu.cam.dodoor.datastore;

import edu.cam.dodoor.thrift.DataStoreService;
import edu.cam.dodoor.thrift.TNodeState;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;

public class DataStoreThrift implements DataStoreService.Iface {
    List<TNodeState> nodeStates;

    void initialize() throws TException {
        nodeStates = new ArrayList<TNodeState>();
    }

    @Override
    public boolean updateNodeLoad(TNodeState nodeStates) throws TException {
        return false;
    }

    @Override
    public List<TNodeState> getNodeStates() throws TException {
        return List.of(new TNodeState());
    }
}
