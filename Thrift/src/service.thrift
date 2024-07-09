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

# SchedulerService is used by application frontends to communicate with Dodoor
# and place jobs.
service SchedulerService {
  # Submit a job composed of a list of individual tasks.
  void submitJob(1: types.TSchedulingRequest req) throws (1: types.IncompleteRequestException e);
  void updateNodeState(1: map<string, types.TNodeState> snapshot);
}

service DataStoreService {
  # Register a scheduler with the given socket address (IP: Port)
  void registerScheduler(1: string schedulerAddress);
  void updateNodeLoad(1:string nodeMonitorAddress, 2:types.TNodeState nodeStates, 3:i32 numTasks);
  map<string, types.TNodeState> getNodeStates();
}

# A service worked as worker nodes to communicate with scheduler
service NodeMonitorService {
  void registerDataStore(1: string dataStoreAddress);
  # Inform the NodeMonitor that a particular task has finished
  void tasksFinished(1: list<types.TFullTaskId> tasks);
}


service InternalService {
  # Enqueues a reservation to launch the given number of tasks. The NodeMonitor sends
  # a GetTask() RPC to the given schedulerAddress when it is ready to launch a task, for each
  # enqueued task reservation. Returns whether or not the task was successfully enqueued.
  bool enqueueTaskReservations(1: types.TEnqueueTaskReservationsRequest request);

  # Cancels reservations for jobs for which all tasks have already been launched.
  void cancelTaskReservations(1: types.TCancelTaskReservationsRequest request);
}