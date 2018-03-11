package com.mesosphere.sdk.framework;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.TextFormat;
import com.mesosphere.sdk.offer.LoggingUtils;
import com.mesosphere.sdk.offer.evaluate.placement.IsLocalRegionRule;
import com.mesosphere.sdk.scheduler.MesosEventClient;
import com.mesosphere.sdk.scheduler.Metrics;
import com.mesosphere.sdk.scheduler.SchedulerConfig;
import com.mesosphere.sdk.scheduler.SchedulerUtils;
import com.mesosphere.sdk.scheduler.MesosEventClient.StatusResponse;
import com.mesosphere.sdk.state.FrameworkStore;
import com.mesosphere.sdk.state.StateStoreException;
import com.mesosphere.sdk.storage.Persister;

/**
 * Implementation of Mesos' {@link Scheduler} interface. There should only be one of these per Scheduler process.
 * Received messages are forwarded to the provided {@link MesosEventClient} instance.
 */
public class FrameworkScheduler implements Scheduler {

    private static final Logger LOGGER = LoggingUtils.getLogger(FrameworkScheduler.class);

    /**
     * Mesos may call registered() multiple times in the lifespan of a Scheduler process, specifically when there's
     * master re-election. Avoid performing initialization multiple times, which would cause queues to be stuck.
     */
    private final AtomicBoolean isRegisterStarted = new AtomicBoolean(false);

    /**
     * Tracks whether the API Server has entered a started state. We avoid launching tasks until after the API server is
     * started, because when tasks launch they typically require access to ArtifactResource for config templates.
     */
    private final AtomicBoolean readyToAcceptOffers = new AtomicBoolean(false);

    private final FrameworkStore frameworkStore;
    private final MesosEventClient mesosEventClient;
    private final OfferProcessor offerProcessor;
    private final ImplicitReconciler implicitReconciler;

    public FrameworkScheduler(SchedulerConfig schedulerConfig, Persister persister, MesosEventClient mesosEventClient) {
        this(
                new FrameworkStore(persister),
                mesosEventClient,
                new OfferProcessor(mesosEventClient, persister),
                new ImplicitReconciler(schedulerConfig));
    }

    @VisibleForTesting
    FrameworkScheduler(
            FrameworkStore frameworkStore,
            MesosEventClient mesosEventClient,
            OfferProcessor offerProcessor,
            ImplicitReconciler implicitReconciler) {
        this.frameworkStore = frameworkStore;
        this.mesosEventClient = mesosEventClient;
        this.offerProcessor = offerProcessor;
        this.implicitReconciler = implicitReconciler;
    }

    /**
     * Returns the framework ID currently in persistent storage, or an empty {@link Optional} if no framework ID had
     * been stored yet.
     *
     * @throws StateStoreException if storage access fails
     */
    Optional<Protos.FrameworkID> fetchFrameworkId() {
        return frameworkStore.fetchFrameworkId();
    }

    /**
     * Notifies this instance that the API server has been initialized. All offers are declined until this is called.
     *
     * @return {@code this}
     */
    public FrameworkScheduler setReadyToAcceptOffers() {
        readyToAcceptOffers.set(true);
        return this;
    }

    /**
     * Disables multithreading for tests. For this to take effect, it must be invoked before the framework has
     * registered.
     *
     * @return {@code this}
     */
    @VisibleForTesting
    public FrameworkScheduler disableThreading() {
        offerProcessor.disableThreading();
        implicitReconciler.disableThreading();
        return this;
    }

    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
        if (isRegisterStarted.getAndSet(true)) {
            // This may occur as the result of a master election.
            LOGGER.info("Already registered, calling reregistered()");
            reregistered(driver, masterInfo);
            return;
        }

        LOGGER.info("Registered framework with frameworkId: {}", frameworkId.getValue());
        try {
            frameworkStore.storeFrameworkId(frameworkId);
        } catch (Exception e) {
            LOGGER.error(String.format(
                    "Unable to store registered framework ID '%s'", frameworkId.getValue()), e);
            SchedulerUtils.hardExit(ExitCode.REGISTRATION_FAILURE, e);
        }

        updateDriverAndDomain(driver, masterInfo);
        mesosEventClient.registered(false);

        // Start background threads:
        offerProcessor.start();
        implicitReconciler.start();
    }

    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
        LOGGER.info("Re-registered with master: {}", TextFormat.shortDebugString(masterInfo));
        updateDriverAndDomain(driver, masterInfo);
        mesosEventClient.registered(true);
    }

    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        Metrics.incrementReceivedOffers(offers.size());

        if (!readyToAcceptOffers.get()) {
            LOGGER.info("Declining {} offer{}: Waiting for API Server to start.",
                    offers.size(), offers.size() == 1 ? "" : "s");
            OfferProcessor.declineShort(offers);
            return;
        }

        offerProcessor.enqueue(offers);
    }

    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        LOGGER.info("Received status update for taskId={} state={} message={} protobuf={}",
                status.getTaskId().getValue(),
                status.getState().toString(),
                status.getMessage(),
                TextFormat.shortDebugString(status));
        Metrics.record(status);
        StatusResponse response = mesosEventClient.status(status);
        boolean eligibleToKill = TaskKiller.update(status);
        switch (response.result) {
        case UNKNOWN_TASK:
            if (eligibleToKill) {
                LOGGER.info("Got unknown task in response to status update, marking task to be killed: {}",
                        status.getTaskId().getValue());
                TaskKiller.killTask(status.getTaskId());
            } else {
                // Special case: Mesos can send TASK_LOST+REASON_RECONCILIATION as a response to a prior kill request
                // against an unknown task. When this happens, we don't want to repeat the kill, because that would
                // result create a Kill -> Status -> Kill -> ... loop
                LOGGER.warn("Got unknown task in response to status update, but task should not be killed again: {}",
                        status.getTaskId().getValue());
            }
            break;
        case PROCESSED:
            // No-op
            break;
        }
    }

    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
        LOGGER.info("Rescinding offer: {}", offerId.getValue());
        offerProcessor.dequeue(offerId);
    }

    @Override
    public void frameworkMessage(
            SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID agentId, byte[] data) {
        LOGGER.error("Received unsupported {} byte Framework Message from Executor {} on Agent {}",
                data.length, executorId.getValue(), agentId.getValue());
    }

    @Override
    public void disconnected(SchedulerDriver driver) {
        LOGGER.error("Disconnected from Master, shutting down.");
        SchedulerUtils.hardExit(ExitCode.DISCONNECTED);
    }

    @Override
    public void slaveLost(SchedulerDriver driver, Protos.SlaveID agentId) {
        LOGGER.warn("Agent lost: {}", agentId.getValue());
    }

    @Override
    public void executorLost(
            SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID agentId, int status) {
        LOGGER.warn("Lost Executor: {} on Agent: {}", executorId.getValue(), agentId.getValue());
    }

    @Override
    public void error(SchedulerDriver driver, String message) {
        LOGGER.error("SchedulerDriver returned an error, shutting down: {}", message);
        SchedulerUtils.hardExit(ExitCode.ERROR);
    }

    private static void updateDriverAndDomain(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
        Driver.setDriver(driver);
        if (masterInfo.hasDomain()) {
            IsLocalRegionRule.setLocalDomain(masterInfo.getDomain());
        }
    }
}
