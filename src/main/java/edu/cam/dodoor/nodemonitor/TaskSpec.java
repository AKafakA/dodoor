package edu.cam.dodoor.nodemonitor;

import edu.cam.dodoor.thrift.TEnqueueTaskReservationsRequest;
import edu.cam.dodoor.thrift.TUserGroupInfo;

import java.net.InetSocketAddress;

/**
 * Class define a task to be scheduled to be run within the nodes
 */
public class TaskSpec {
    public TUserGroupInfo _user;
    public String _requestId;

    /**
     * ID of the task that previously ran in the slot this task is using. Used
     * to track how long it takes to fill an empty slot on a slave. Empty if this task was launched
     * immediately, because there were empty slots available on the slave.  Filled in when
     * the task is launched.
     */
    public String _previousRequestId;
    public String _previousTaskId;

    public int _cores;
    public long _memory;
    public long _disks;
    public long _duration;


    public TaskSpec(TEnqueueTaskReservationsRequest request) {
        _user = request.getUser();
        _requestId = request.taskId;
        _previousRequestId = "";
        _previousTaskId = "";

        _cores = request.resourceRequested.cores;
        _memory = request.resourceRequested.memory;
        _disks = request.resourceRequested.disks;
        _duration = request.durationInMs;
    }
}
