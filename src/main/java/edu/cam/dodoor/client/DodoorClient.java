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

    private AtomicInteger _nextRequestId = new AtomicInteger(0);

    public void initialize(InetSocketAddress sparrowSchedulerAddr,
                           Configuration conf)
            throws TException, IOException {
        initialize(sparrowSchedulerAddr, DEFAULT_LISTEN_PORT, conf);
    }

    public void initialize(InetSocketAddress schedulerAddr, int listenPort, Configuration conf)
            throws TException, IOException {

        for (int i = 0; i < NUM_CLIENTS; i++) {
            SchedulerService.Client client = TClients.createBlockingSchedulerClient(
                    schedulerAddr.getAddress().getHostAddress(), schedulerAddr.getPort(),
                    60000);
            _clients.add(client);
        }
    }

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
