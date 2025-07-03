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
import edu.cam.dodoor.utils.ThriftClientPool;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import edu.cam.dodoor.thrift.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.pool.impl.GenericKeyedObjectPool.Config;
import org.apache.thrift.async.AsyncMethodCallback;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe Java API to Dodoor scheduling service.
 */
public class DodoorClient {

    private final static Logger LOG = Logger.getLogger(DodoorClient.class);
    private int _numClients;

    private ThriftClientPool<SchedulerService.AsyncClient> _clientPool;

    private AtomicInteger _nextRequestId = new AtomicInteger(0);

    private static TUserGroupInfo DEFAULT_USER = new TUserGroupInfo("default", "normal", 1);

    private static TPlacementPreference NO_PREFERENCE = new TPlacementPreference(new ArrayList<>());

    private List<InetSocketAddress> _schedulerAddresses;

    public void initialize(InetSocketAddress[] schedulerAddresses, Configuration config)
            throws TException, IOException {

        int numSchedulers = schedulerAddresses.length;
        _numClients = config.getInt(DodoorConf.DODOOR_NUM_SCHEDULER_CLIENTS_PER_PORT,
                DodoorConf.DEFAULT_DODOOR_NUM_SCHEDULER_CLIENTS_PER_PORT);

        Config poolConfig = ThriftClientPool.getPoolConfig();
        poolConfig.maxTotal = _numClients;
        _clientPool = new ThriftClientPool<>(new ThriftClientPool.SchedulerServiceMakerFactory(), poolConfig);
        _schedulerAddresses = Arrays.asList(schedulerAddresses);
    }

    public void submitTask(String taskId, int cores, long memory, long disks, long durationInMs,
                           String taskType)
            throws TException {
        List<TTaskSpec> tasks = new ArrayList<>();
        TResourceVector resources = new TResourceVector(cores, memory, disks);
        TTaskSpec task = new TTaskSpec(taskId,
                NO_PREFERENCE,
                java.nio.ByteBuffer.wrap("".getBytes()),
                resources,
                durationInMs,
                taskType);
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

    public synchronized boolean submitRequest(TSchedulingRequest request) {
        try {
            InetSocketAddress schedulerAddr = _schedulerAddresses.get(_nextRequestId.getAndIncrement() % _schedulerAddresses.size());
            SchedulerService.AsyncClient client = _clientPool.borrowClient(schedulerAddr);
            client.submitJob(request, new JobSubmissionCallback(schedulerAddr, request, client));
        } catch (InterruptedException e) {
            LOG.fatal(e);
        } catch (TException e) {
            LOG.error("Thrift exception when submitting job: " + e.getMessage());
            return false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    private class JobSubmissionCallback implements AsyncMethodCallback<Void> {
        private final InetSocketAddress _schedulerAddr;
        private final SchedulerService.AsyncClient _client;
        private final TSchedulingRequest _request;


        public JobSubmissionCallback(InetSocketAddress schedulerAddr, TSchedulingRequest request,
                                     SchedulerService.AsyncClient client) {
            _schedulerAddr = schedulerAddr;
            _client = client;
            _request = request;
        }


        @Override
        public void onComplete(Void unused) {
            LOG.info("Job submitted: " + _request.requestId);
            try {
                _clientPool.returnClient(_schedulerAddr, _client);
            } catch (Exception e) {
                LOG.error("Error returning client to pool", e);
            }

        }

        @Override
        public void onError(Exception error) {
            LOG.error("Error submitting job", error);
            try {
                _clientPool.returnClient(_schedulerAddr, _client);
            } catch (Exception e) {
                LOG.error("Error returning client to pool", e);
            }
        }
    }
}
