package edu.cam.dodoor.nodemonitor;

import edu.cam.dodoor.thrift.TEnqueueTaskReservationsRequest;
import edu.cam.dodoor.thrift.TTaskLaunchSpec;
import edu.cam.dodoor.thrift.TUserGroupInfo;

import java.net.InetSocketAddress;

/**
 * Class define a task to be scheduled to be run within the nodes
 */
public class TaskSpec {
    public String _appId;
    public TUserGroupInfo _user;
    public String _requestId;

    public InetSocketAddress _schedulerAddress;
    public InetSocketAddress _appBackendAddress;

    /**
     * ID of the task that previously ran in the slot this task is using. Used
     * to track how long it takes to fill an empty slot on a slave. Empty if this task was launched
     * immediately, because there were empty slots available on the slave.  Filled in when
     * the task is launched.
     */
    public String _previousRequestId;
    public String _previousTaskId;

    /** Filled in after the getTask() RPC completes. */
    public TTaskLaunchSpec _taskSpec;

    public TaskSpec(TEnqueueTaskReservationsRequest request, InetSocketAddress appBackendAddress) {
        _appId = request.getAppId();
        _user = request.getUser();
        _requestId = request.getRequestId();
        _schedulerAddress = new InetSocketAddress(request.getSchedulerAddress().getHost(),
                request.getSchedulerAddress().getPort());
        _appBackendAddress = appBackendAddress;
        _previousRequestId = "";
        _previousTaskId = "";
    }
}
