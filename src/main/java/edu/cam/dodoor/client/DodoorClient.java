/*
 * Copyright 2024 University of Cambridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.cam.dodoor.client;

import edu.cam.dodoor.DodoorConf;
import edu.cam.dodoor.utils.TClients;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import edu.cam.dodoor.thrift.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe Java API to Dodoor scheduling service.
 */
public class DodoorClient {

    private final static Logger LOG = Logger.getLogger(DodoorClient.class);
    private int _numClients;

    BlockingQueue<SchedulerService.Client> _clients =
            new LinkedBlockingQueue<>();

    private AtomicInteger _nextRequestId = new AtomicInteger(0);

    private static TUserGroupInfo DEFAULT_USER = new TUserGroupInfo("default", "normal", 1);

    private static TPlacementPreference NO_PREFERENCE = new TPlacementPreference(new ArrayList<>());

    public void initialize(InetSocketAddress[] schedulerAddresses, Configuration config)
            throws TException, IOException {

        int numSchedulers = schedulerAddresses.length;
        _numClients = config.getInt(DodoorConf.DODOOR_NUM_SCHEDULER_CLIENTS_PER_PORT,
                DodoorConf.DEFAULT_DODOOR_NUM_SCHEDULER_CLIENTS_PER_PORT);

        for (int i = 0; i < _numClients; i++) {
            InetSocketAddress schedulerAddr = schedulerAddresses[i % numSchedulers];
            SchedulerService.Client client = TClients.createBlockingSchedulerClient(
                    schedulerAddr.getAddress().getHostAddress(), schedulerAddr.getPort(),
                    60000);
            _clients.add(client);
        }
    }

    public void submitTask(String taskId, int cores, long memory, long disks, long durationInMs)
            throws TException {
        List<TTaskSpec> tasks = new ArrayList<>();
        TResourceVector resources = new TResourceVector(cores, memory, disks);
        TTaskSpec task = new TTaskSpec(taskId,
                NO_PREFERENCE,
                java.nio.ByteBuffer.wrap("".getBytes()),
                resources,
                durationInMs);
        tasks.add(task);
        submitJob(tasks);
    }

    public void submitJob(List<TTaskSpec> tasks)
            throws TException {
        submitRequest(new TSchedulingRequest(tasks, DEFAULT_USER, _nextRequestId.getAndIncrement()));
    }

    /**
     * Can be used in the future from another trace player (e.g JobTracePlayer) to submit a job with set of requests instead
     */
    public boolean submitJob(List<TTaskSpec> tasks, TUserGroupInfo user)
            throws TException {
        return submitRequest(new TSchedulingRequest(tasks, user, _nextRequestId.getAndIncrement()));
    }

    public boolean submitJob(List<TTaskSpec> tasks,
                             TUserGroupInfo user, String description) throws TException {
        TSchedulingRequest request = new TSchedulingRequest(tasks, user, _nextRequestId.getAndIncrement());
        request.setDescription(description);
        return submitRequest(request);
    }

    public boolean submitJob(List<TTaskSpec> tasks, TUserGroupInfo user,
                             double probeRatio)
            throws TException {
        TSchedulingRequest request = new TSchedulingRequest(tasks, user, _nextRequestId.getAndIncrement());
        return submitRequest(request);
    }

    public boolean submitRequest(TSchedulingRequest request) throws TException {
        try {
            SchedulerService.Client client = _clients.take();
            client.submitJob(request);
            _clients.put(client);
        } catch (InterruptedException e) {
            LOG.fatal(e);
        } catch (TException e) {
            LOG.error("Thrift exception when submitting job: " + e.getMessage());
            return false;
        }
        return true;
    }
}
