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
  1: i32 cores;       // # Cores
  2: i64 memory;      // Memory, in Mb
  3: i64 disks;
}


// A fully-specified Task
struct TFullTaskId {
  1: string taskId;
  2: TResourceVector resourceRequest;
  3: i64 durationInMs;
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
  # The placement preference for selected noded for the task, not been implemented so far.
  2: TPlacementPreference preference;
  3: binary message;
  4: TResourceVector resourceRequest;
  5: i64 durationInMs;
}

struct TSchedulingRequest {
  1: list<TTaskSpec> tasks;
  # Not been implemented with user level priority
  2: TUserGroupInfo user;
  # A description that will be logged alongside the requestId that Pigeon assigns.
  3: optional string description;
  4: i64 requestId;
}

# Represents the State Store's view of resource consumption on a node.
struct TNodeState {
  1: TResourceVector resourceRequested;  # Resources has been requested and pending
  2: i32 numTasks;  # Number of tasks running on the node
  3: i64 totalDurations;  # Total duration of all tasks pending on the node
}

exception ServerNotReadyException {
    1: string message; # Thrown when master has less than one HW/LW
}

struct TEnqueueTaskReservationRequest {
  1: TUserGroupInfo user;
  2: string taskId;
  3: THostPort schedulerAddress;
  4: TResourceVector resourceRequested;
  5: i64 durationInMs;
  6: THostPort nodeEnqueueAddress;
}