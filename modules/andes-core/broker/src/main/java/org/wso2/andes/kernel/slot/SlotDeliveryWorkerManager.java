/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel.slot;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.MessageFlusher;
import org.wso2.andes.kernel.MessagingEngine;
import org.wso2.andes.kernel.subscription.StorageQueue;
import org.wso2.andes.server.cluster.error.detection.NetworkPartitionListener;
import org.wso2.andes.store.FailureObservingStoreManager;
import org.wso2.andes.store.HealthAwareStore;
import org.wso2.andes.store.StoreHealthListener;
import org.wso2.andes.task.TaskExecutorService;

import java.util.concurrent.ThreadFactory;

/**
 * This class is responsible of allocating SloDeliveryWorker threads to each queue
 */
public final class SlotDeliveryWorkerManager implements StoreHealthListener, NetworkPartitionListener,
        CoordinatorConnectionListener {

    private static Log log = LogFactory.getLog(SlotDeliveryWorkerManager.class);

    /**
     * Delay for waiting for an idle task
     */
    private static final long IDLE_TASK_DELAY_MILLIS = 100;

    /**
     * Slot Delivery Worker Manager instance
     */
    private static SlotDeliveryWorkerManager slotDeliveryWorkerManager = new SlotDeliveryWorkerManager();

    private final TaskExecutorService<MessageDeliveryTask> taskManager;

    private SlotDeliveryWorkerManager() {
        int numberOfThreads = AndesConfigurationManager
                .readValue(AndesConfiguration.PERFORMANCE_TUNING_SLOTS_WORKER_THREAD_COUNT);

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("MessageDeliveryTaskThreadPool-%d").build();

        taskManager = new TaskExecutorService<>(numberOfThreads, IDLE_TASK_DELAY_MILLIS, threadFactory);
        taskManager.setExceptionHandler(new DeliveryTaskExceptionHandler());
        AndesContext andesContext = AndesContext.getInstance();

        if (andesContext.isClusteringEnabled()) {
            // network partition detection and thrift client works only when clustered.
            andesContext.getClusterAgent().addNetworkPartitionListener(50, this);
            MessagingEngine.getInstance().getSlotCoordinator().addCoordinatorConnectionListener(this);
        }

        FailureObservingStoreManager.registerStoreHealthListener(this);
    }

    /**
     * @return SlotDeliveryWorkerManager instance
     */
    public static SlotDeliveryWorkerManager getInstance() {
        return slotDeliveryWorkerManager;
    }

    /**
     * When a subscription is added this method will be called. if this is the first subscriber for the destination
     * a {@link MessageDeliveryTask} will be added to the {@link TaskExecutorService}
     *
     * @param storageQueue queue to start slot delivery worker for
     */
    public void startMessageDeliveryForQueue(StorageQueue storageQueue) throws AndesException {

        MessageDeliveryTask messageDeliveryTask = new MessageDeliveryTask(storageQueue,
                                    MessagingEngine.getInstance().getSlotCoordinator(),
                                        MessageFlusher.getInstance());
        taskManager.add(messageDeliveryTask);
    }

    /**
     * Stop delivery task for the given storage queue locally.
     * This is normally called when all the subscribers for a
     * destination leave the local node.
     *
     * @param storageQueue Storage queue to stop delivery for
     */
    public void stopDeliveryForQueue(StorageQueue storageQueue) {
        String storageQueueName = storageQueue.getName();
        if (log.isDebugEnabled()) {
            log.debug("Stopping delivery for storage queue " + storageQueueName + " with MessageDeliveryTask : "
                              + storageQueueName);
        }
        taskManager.remove(storageQueueName);
    }

    /**
     * Stop all stop delivery workers in the thread pool
     */
    public void stopMessageDelivery() {
        taskManager.shutdown();
    }

    /**
     * Start all the SlotDeliveryWorkers if not already in running state.
     */
    public void startMessageDelivery() {
        taskManager.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void minimumNodeCountNotFulfilled(int currentNodeCount) {
        log.warn("Network outage detected therefore stopping message delivery. Current cluster size "
                         + currentNodeCount);
        taskManager.stop();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void minimumNodeCountFulfilled(int currentNodeCount) {
        log.info("Network outage resolved therefore resuming message delivery. Current cluster size "
                         + currentNodeCount);
        taskManager.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clusteringOutage() {
        log.warn("Clustering outage. Stopping message delivery");
        taskManager.stop();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeNonOperational(HealthAwareStore store, Exception ex) {
        log.warn("Message stores became not operational therefore waiting");
        taskManager.stop();

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeOperational(HealthAwareStore store) {
        log.info("Message stores became operational therefore resuming work");
        taskManager.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onCoordinatorDisconnect() {
        log.warn("Disconnected from the coordinator. Waiting till reconnect");
        taskManager.stop();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onCoordinatorReconnect() {
        log.info("Coordinator connection re-established");
        taskManager.start();
    }
}
