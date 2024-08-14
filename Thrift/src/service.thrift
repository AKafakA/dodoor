#
# Copyright 2024 Univeristy of Cambridge
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

include 'types.thrift'

namespace java edu.cam.dodoor.thrift

# SchedulerService for scheduling tasks
service SchedulerService {
  # Submit a job composed of a list of individual tasks.
  void submitJob(1: types.TSchedulingRequest req) throws (1: types.IncompleteRequestException e);
  # Update the a view of state of the nodes for scheduling decisions
  void updateNodeState(1: map<string, types.TNodeState> snapshot);
  # Register a node with the given socket address for enqueue and monitoring (IP: nmPort:nePort)
  void registerNode(1: string nodeFullAddress);
    # Register a datastore with the given socket address (IP: Port)
  void registerDataStore(1: string dataStoreAddress);
  # used to caculate the end 2 end latency
  void taskFinished(1: types.TFullTaskId task);
}

# DataStoreService for storing the state of the nodes
service DataStoreService {
  # Register a scheduler with the given socket address (IP: Port)
  void registerScheduler(1: string schedulerAddress);
  # Register a node with the given socket address for enqueue and monitoring (IP: nmPort:nePort)
  void registerNode(1: string nodeFullAddress);
  # Update the state of the nodes for scheduling decisions by given the node enqueue socket address and the node state
  void overrideNodeState(1:string nodeEnqueueAddress, 2:types.TNodeState nodeState);
  # Add the load of set of tasks to the node specified by the nodeEnqueueAddress
  void addNodeLoads(1: map<string, types.TNodeState> additionNodeStates, 2: i32 sign);

  map<string, types.TNodeState> getNodeStates();
}


# Service of the node exposed for querying state and registering with the DataStore
# Two services are exposed to the node: NodeMonitorService and NodeEnqueueService
# which allow the nodes to be queried synchronously for realtime probing and request asynchronously for cached based approach
service NodeMonitorService {
   # Register a datastore with the given socket address (IP: Port)
  void registerDataStore(1: string dataStoreAddress);
  # called by the scheduler to get the number of tasks running on the node for sparrow test
  i32 getNumTasks();
}

# Service of the node exposed to the scheduler to enqueue tasks
service NodeEnqueueService {
  bool enqueueTaskReservation(1: types.TEnqueueTaskReservationRequest request);
  void taskFinished(1: types.TFullTaskId task);
}