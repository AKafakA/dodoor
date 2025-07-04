package edu.cam.dodoor.datastore;

import com.google.common.collect.Maps;
import edu.cam.dodoor.thrift.TNodeState;
import edu.cam.dodoor.thrift.TResourceVector;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Basic implementation for {@link DataStore} to have the node states stored inside memory as List
 */
public class BasicDataStoreImpl implements DataStore{

    private static final Logger LOG = LoggerFactory.getLogger(BasicDataStoreImpl.class);
    private final ConcurrentMap<String, TNodeState> _nodeStates;


    public BasicDataStoreImpl(){
        _nodeStates = Maps.newConcurrentMap();
    }

    @Override
    public void initialize(Configuration config) {
    }

    @Override
    public void overrideNodeLoad(String nodeEnqueueAddress, TNodeState nodeStates) {
        LOG.debug("Updating node load for {}", nodeEnqueueAddress);
        _nodeStates.put(nodeEnqueueAddress, nodeStates);
    }

    @Override
    public void addNodeLoads(Map<String, TNodeState> additionalNodeLoad, int sign) {
        for (Map.Entry<String, TNodeState> entry : additionalNodeLoad.entrySet()) {
            String nodeEnqueueAddress = entry.getKey();
            TNodeState nodeState = entry.getValue();
            addSingleNodeLoad(nodeEnqueueAddress, nodeState.resourceRequested, nodeState.numTasks,
                    nodeState.totalDurations, sign, nodeState.nodeType);
        }
    }

    private synchronized void addSingleNodeLoad(String nodeEnqueueAddress, TResourceVector resourceVector,
                                                int numTasks, long newTotalDurations, int sign,
                                                String nodeType) {
        TNodeState nodeState = _nodeStates.get(nodeEnqueueAddress);
        String nodeIp = nodeEnqueueAddress.split(":")[0];
        if (nodeState == null) {
            LOG.warn("Node {} not found in the data store. Creating a new entry.", nodeEnqueueAddress);
            nodeState = new TNodeState(new TResourceVector(), 0, 0, nodeIp, nodeType);
        } else if (!nodeState.nodeIp.equals(nodeIp)) {
           LOG.error("Node {} already exists in the data store but with different IP address {}",
                   nodeEnqueueAddress, nodeState.nodeIp);
        }
        if (numTasks < 0 || Math.abs(sign) != 1) {
            throw new IllegalArgumentException("numTasks should be positive and sign should be 1 or -1");
        }

        TResourceVector existedResources = nodeState.resourceRequested;
        existedResources.cores = resourceVector.cores * sign + existedResources.cores;
        existedResources.memory = resourceVector.memory * sign + existedResources.memory;
        existedResources.disks = resourceVector.disks * sign + existedResources.disks;
        nodeState.numTasks = numTasks * sign + nodeState.numTasks;
        nodeState.totalDurations = newTotalDurations * sign + nodeState.totalDurations;
        _nodeStates.put(nodeEnqueueAddress, nodeState);
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
