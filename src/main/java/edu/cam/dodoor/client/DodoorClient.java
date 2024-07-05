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
import edu.cam.dodoor.utils.TServers;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import edu.cam.dodoor.thrift.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe Java API to Dodoor scheduling service.
 */
public class DodoorClient {
    public static boolean _launchedServerAlready = false;

    private final static Logger LOG = Logger.getLogger(DodoorClient.class);
    private final static int NUM_CLIENTS = 8;
    private final static int DEFAULT_LISTEN_PORT = 50201;

    BlockingQueue<SchedulerService.Client> _clients =
            new LinkedBlockingQueue<>();

    DataStoreService.Client _dataStoreClient;
    private int _batchSize;
    private AtomicInteger _counter;

    public void initialize(InetSocketAddress sparrowSchedulerAddr,
                           FrontendService.Iface frontendServer,
                           Configuration conf)
            throws TException, IOException {
        initialize(sparrowSchedulerAddr, frontendServer, DEFAULT_LISTEN_PORT, conf);
    }

    public void initialize(InetSocketAddress schedulerAddr,
                           FrontendService.Iface frontendServer, int listenPort, Configuration conf)
            throws TException, IOException {

        FrontendService.Processor<FrontendService.Iface> processor =
                new FrontendService.Processor<>(frontendServer);

        if (!_launchedServerAlready) {
            try {
                TServers.launchThreadedThriftServer(listenPort, NUM_CLIENTS, processor);
            } catch (IOException e) {
                LOG.fatal("Couldn't launch server side of frontend", e);
            }
            _launchedServerAlready = true;
        }

        for (int i = 0; i < NUM_CLIENTS; i++) {
            SchedulerService.Client client = TClients.createBlockingSchedulerClient(
                    schedulerAddr.getAddress().getHostAddress(), schedulerAddr.getPort(),
                    60000);
            _clients.add(client);
        }
        _counter = new AtomicInteger(0);
        _batchSize = conf.getInt(DodoorConf.BATCH_SIZE, DodoorConf.DEFAULT_BATCH_SIZE);
    }

    public boolean submitJob(List<TTaskSpec> tasks, TUserGroupInfo user)
            throws TException {
        return submitRequest(new TSchedulingRequest(tasks, user));
    }

    public boolean submitJob(List<TTaskSpec> tasks,
                             TUserGroupInfo user, String description) throws TException {
        TSchedulingRequest request = new TSchedulingRequest(tasks, user);
        request.setDescription(description);
        return submitRequest(request);
    }

    public boolean submitJob(List<TTaskSpec> tasks, TUserGroupInfo user,
                             double probeRatio)
            throws TException {
        TSchedulingRequest request = new TSchedulingRequest(tasks, user);
        return submitRequest(request);
    }

    public boolean submitRequest(TSchedulingRequest request) throws TException {
        try {
            SchedulerService.Client client = _clients.take();
            client.submitJob(request);
            _clients.put(client);
            _counter.getAndAdd(request.tasks.size());
        } catch (InterruptedException e) {
            LOG.fatal(e);
        } catch (TException e) {
            LOG.error("Thrift exception when submitting job: " + e.getMessage());
            return false;
        }

        if (_counter.get() >= _batchSize) {
            Map<String, TNodeState> nodeStates = _dataStoreClient.getNodeStates();
            for (SchedulerService.Client client : _clients) {
                client.updateNodeState(nodeStates);
            }
        }

        _counter.set(0);

        return true;
    }

}
