# Dodoor
# Copyright 2024 Univeristy of Cambridge
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

namespace java edu.cam.dodoor.thrift

exception IncompleteRequestException {
  1: string message;
}

struct THostPort {
  // The host should always be represented as an IP address!
  1: string host;
  2: i32 port;
}

struct TPlacementPreference {
  1: list<string> nodes; // List of preferred nodes, described by their hostname.
}

struct TResourceVector {
  1: double cores;       // # Cores
  2: i64 memory;      // Memory, in Mb
  3: i64 disks;
}


// A fully-specified Task
struct TFullTaskId {
  1: string taskId;    // Task ID as reported from the FE
  2: string requestId; // Scheduling request ID as assigned by the FE
  3: THostPort schedulerAddress; // Address of the scheduler that scheduled the task.
  4: string appId;
  5: TResourceVector resourceRequest;
}

struct TUserGroupInfo {
  1: string user;
  2: string group;
  // Priority of the user. If the node monitor is using the priority task scheduler,
  // it will place the tasks with the smallest numbered priority first.
  3: i32 priority;
}

struct TTaskSpec {
  1: string taskId;
  2: TPlacementPreference preference;
  3: binary message;
  4: TResourceVector resourceRequest;
}

struct TSchedulingRequest {
  1: list<TTaskSpec> tasks;
  2: TUserGroupInfo user;
  # A description that will be logged alongside the requestId that Pigeon assigns.
  3: optional string description;
}

struct TLaunchTasksRequest {
    1: TUserGroupInfo user;
    2: string requestID;
    3: THostPort schedulerAddress;
    4: list<TTaskLaunchSpec> tasksToBeLaunched;
}

# Information needed to launch a task.
struct TTaskLaunchSpec {
  # Task ID
  1: string taskId;

  # Description of the task passed on to the application backend.
  2: binary message;
  3: TResourceVector resourceRequested;
}

# Represents the State Store's view of resource consumption on a node.
struct TNodeState {
  1: TResourceVector resourceUsed;   # Resources currently used
  2: TResourceVector resourceRequested;  # Resources has been requested and pending
}

exception ServerNotReadyException {
    1: string message; # Thrown when master has less than one HW/LW
}


struct LoadSpec {
  1: double load;
}

struct TEnqueueTaskReservationsRequest {
  1: string appId;
  2: TUserGroupInfo user;
  3: string requestId;
  4: THostPort schedulerAddress;
  5: i32 numTasks;
}

struct TCancelTaskReservationsRequest {
  1: string requestId;
}