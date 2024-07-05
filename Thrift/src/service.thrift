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

  # Register a frontend for the given application.
  bool registerFrontend(1: string app, 2: string socketAddress);

  # Submit a job composed of a list of individual tasks.
  void submitJob(1: types.TSchedulingRequest req) throws (1: types.IncompleteRequestException e);

  # Send a message to be delivered to the frontend for {app} pertaining
  # to the task {taskId}. The {status} field allows for application-specific
  # status enumerations. Right now this is used only for Spark, which relies on
  # the scheduler to send task completion messages to frontends.

  void sendFrontendMessage(1: string app, 2: types.TFullTaskId taskId,
                           3: i32 status, 4: binary message);

  void updateNodeState(1: map<string, types.TNodeState> snapshot);
}

service DataStoreService {
  # Register a scheduler with the given socket address (IP: Port)
  void registerScheduler(1: string schedulerAddress);

  void updateNodeLoad(1:string nodeMonitorAddress, 2:types.TNodeState nodeStates);

  map<string, types.TNodeState> getNodeStates();
}

# A service that workers backends are expected to extend.
# lauch the scheduled tasks
service BackendService {
  void launchTask(1: binary message, 2: types.TFullTaskId taskId,
                  3: types.TUserGroupInfo user);
}

# A service that workers frontends are expected to extend.
service FrontendService {
  # See SchedulerService.sendFrontendMessage
  void frontendMessage(1: types.TFullTaskId taskId, 2: i32 status,
                       3: binary message);
}

# A service worked as worker nodes to communicate with scheduler
service NodeMonitorService {
  bool registerBackend(1: string app, 2: string listenSocket);

  # Inform the NodeMonitor that a particular task has finished
  void tasksFinished(1: list<types.TFullTaskId> tasks);

  # See SchedulerService.sendFrontendMessage
  void sendFrontendMessage(1: string app, 2: types.TFullTaskId taskId,
                           3: i32 status, 4: binary message);
}


service InternalService {
  # Enqueues a reservation to launch the given number of tasks. The NodeMonitor sends
  # a GetTask() RPC to the given schedulerAddress when it is ready to launch a task, for each
  # enqueued task reservation. Returns whether or not the task was successfully enqueued.
  bool enqueueTaskReservations(1: types.TEnqueueTaskReservationsRequest request);

  # Cancels reservations for jobs for which all tasks have already been launched.
  void cancelTaskReservations(1: types.TCancelTaskReservationsRequest request);
}

service GetTaskService {
  # Called by a node monitor when it has available responses to run a task.
  list<types.TTaskLaunchSpec> getTask(1: string requestId, 2: types.THostPort nodeMonitorAddress);
}


service RecursiveService {

  # The delegated node will hold untill all tasks related to the same request completed to notify the scheduler
   void tasksFinished(1: string requestID, 2:types.THostPort master);
}
