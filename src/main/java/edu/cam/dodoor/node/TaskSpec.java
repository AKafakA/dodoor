package edu.cam.dodoor.node;

import edu.cam.dodoor.thrift.TEnqueueTaskReservationRequest;
import edu.cam.dodoor.thrift.TFullTaskId;
import edu.cam.dodoor.thrift.TResourceVector;
import edu.cam.dodoor.thrift.TUserGroupInfo;


/**
 * Class define a task to be scheduled to be run within the nodes
 */
public class TaskSpec {
    public TUserGroupInfo _user;
    public String _taskId;

    /**
     * ID of the task that previously ran in the slot this task is using. Used
     * to track how long it takes to fill an empty slot on a slave. Empty if this task was launched
     * immediately, because there were empty slots available on the slave.  Filled in when
     * the task is launched.
     */
    public String _previousTaskId;

    public TResourceVector _resourceVector;
    public long _duration;
    public long _enqueuedTime;
    private final TEnqueueTaskReservationRequest _request;
    public String _taskType;


    public TaskSpec(TEnqueueTaskReservationRequest request) {
        _user = request.getUser();
        _taskId = request.taskId;
        _previousTaskId = "";

        _resourceVector = request.resourceRequested;
        _duration = request.durationInMs;

        _enqueuedTime = request.enqueueTime;
        _request = request;
        _taskType = request.taskType;
    }

    public TaskSpec(TEnqueueTaskReservationRequest request, long enqueuedTime) {
        _user = request.getUser();
        _taskId = request.taskId;
        _previousTaskId = "";

        _resourceVector = request.resourceRequested;
        _duration = request.durationInMs;

        _enqueuedTime = enqueuedTime;
        _request = request;
        _taskType = request.taskType;
    }

    public TFullTaskId getFullTaskId() {
        return new TFullTaskId(_taskId, _resourceVector, _duration, _taskType);
    }

    public TEnqueueTaskReservationRequest getRequest() {
        return _request;
    }
}
