package edu.cam.dodoor.datastore;

import edu.cam.dodoor.thrift.TNodeState;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;

import java.util.Map;

public interface DataStore {

    public void initialize(Configuration config) throws TException;

    public void updateNodeLoad(String nodeMonitorAddress, TNodeState nodeStates) throws TException;

    public Map<String, TNodeState> getNodeStates() throws TException ;
}
