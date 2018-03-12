package com.mesosphere.sdk.queues.scheduler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.mesos.Protos;
import org.slf4j.Logger;

import com.google.protobuf.TextFormat;
import com.mesosphere.sdk.http.endpoints.*;
import com.mesosphere.sdk.http.types.StringPropertyDeserializer;
import com.mesosphere.sdk.offer.CommonIdUtils;
import com.mesosphere.sdk.offer.Constants;
import com.mesosphere.sdk.offer.LoggingUtils;
import com.mesosphere.sdk.offer.OfferRecommendation;
import com.mesosphere.sdk.offer.OfferUtils;
import com.mesosphere.sdk.offer.ResourceUtils;
import com.mesosphere.sdk.offer.TaskException;
import com.mesosphere.sdk.queues.generator.Generator;
import com.mesosphere.sdk.queues.http.endpoints.*;
import com.mesosphere.sdk.queues.http.types.RunInfoProvider;
import com.mesosphere.sdk.queues.state.SpecStore;
import com.mesosphere.sdk.scheduler.MesosEventClient;
import com.mesosphere.sdk.scheduler.OfferResources;
import com.mesosphere.sdk.scheduler.SchedulerConfig;
import com.mesosphere.sdk.scheduler.AbstractScheduler;
import com.mesosphere.sdk.scheduler.plan.DefaultPhase;
import com.mesosphere.sdk.scheduler.plan.DefaultPlan;
import com.mesosphere.sdk.scheduler.plan.DefaultPlanManager;
import com.mesosphere.sdk.scheduler.plan.Plan;
import com.mesosphere.sdk.scheduler.plan.PlanManager;
import com.mesosphere.sdk.scheduler.plan.strategy.SerialStrategy;
import com.mesosphere.sdk.scheduler.uninstall.DeregisterStep;

/**
 * Mesos client which wraps running runs, routing Mesos events to the appropriate runs.
 */
public class QueueEventClient implements MesosEventClient {

    /**
     * Interface for notifying the caller that added runs have completed uninstall.
     */
    public interface UninstallCallback {
        /**
         * Invoked when a given run has completed its uninstall as triggered by
         * {@link QueueEventClient#uninstallRun(String)}. After this has been called, re-adding the run to the
         * {@link QueueEventClient} will result in launching a new instance from scratch.
         */
        void uninstalled(String runName);
    }

    private static final Logger LOGGER = LoggingUtils.getLogger(QueueEventClient.class);

    private final SpecStore specStore;
    private final DefaultRunManager runManager;
    private final Map<String, Generator> runGenerators;
    private final Optional<String> defaultSpecType;
    private final UninstallCallback uninstallCallback;
    private final DeregisterStep deregisterStep;
    private final Optional<Plan> uninstallPlan;

    public QueueEventClient(
            SchedulerConfig schedulerConfig,
            SpecStore specStore,
            DefaultRunManager runManager,
            Map<String, Generator> runGenerators,
            Optional<String> defaultSpecType,
            UninstallCallback uninstallCallback) {
        this.specStore = specStore;
        this.runManager = runManager;
        this.runGenerators = runGenerators;
        this.defaultSpecType = defaultSpecType;
        this.uninstallCallback = uninstallCallback;

        if (schedulerConfig.isUninstallEnabled()) {
            this.deregisterStep = new DeregisterStep();
            this.uninstallPlan = Optional.of(
                    new DefaultPlan(Constants.DEPLOY_PLAN_NAME, Collections.singletonList(
                            new DefaultPhase(
                                    "deregister-framework",
                                    Collections.singletonList(deregisterStep),
                                    new SerialStrategy<>(),
                                    Collections.emptyList()))));
        } else {
            this.deregisterStep = null;
            this.uninstallPlan = Optional.empty();
        }
    }

    @Override
    public void registered(boolean reRegistered) {
        runManager.registered(reRegistered);
    }

    @Override
    public void unregistered() {
        if (!uninstallPlan.isPresent()) {
            // This should have only happened after we returned OfferResponse.finished() below
            throw new IllegalStateException("unregistered() called, but the we are not uninstalling");
        }
        deregisterStep.setComplete();
    }

    /**
     * Forwards the provided offer(s) to all enclosed services, seeing which services are interested in them.
     *
     * TODO(data-agility): Lots of opportunities to optimize this. Needs benchmarks. For example:
     * <ul>
     * <li>- Hide reserved resources from services that they don't belong to</li>
     * <li>- Forward the offers to a random ordering of services to avoid some services starving others</li>
     * <li>- Distribute the offers across all services in parallel (optimistic offers)</li>
     * <li>- ... Pretty much anything that you could see Mesos itself doing.</li>
     * </ul>
     */
    @Override
    public OfferResponse offers(Collection<Protos.Offer> offers) {
        // If uninstalling, then finish uninstall. Otherwise decline short.
        boolean noClients = false;
        // Decline short.
        boolean anyClientsNotReady = false;

        List<OfferRecommendation> recommendations = new ArrayList<>();
        List<Protos.Offer> remainingOffers = new ArrayList<>();
        remainingOffers.addAll(offers);

        Collection<String> finishedRuns = new ArrayList<>();
        Collection<String> uninstalledRuns = new ArrayList<>();

        Collection<AbstractScheduler> runs = runManager.lockAndGetRuns();
        LOGGER.info("Sending {} offer{} to {} service{}:",
                offers.size(), offers.size() == 1 ? "" : "s",
                runs.size(), runs.size() == 1 ? "" : "s");
        try {
            if (runs.isEmpty()) {
                // If we don't have any clients, then WE aren't ready.
                // Decline short, or finish if there's an uninstall in progress.
                noClients = true;
            }
            for (AbstractScheduler run : runs) {
                String runServiceName = run.getServiceSpec().getName();
                OfferResponse response = run.offers(remainingOffers);
                if (!remainingOffers.isEmpty() && !response.recommendations.isEmpty()) {
                    // Some offers were consumed. Update what remains to offer to the next run.
                    List<Protos.Offer> updatedRemainingOffers =
                            OfferUtils.filterOutAcceptedOffers(remainingOffers, response.recommendations);
                    remainingOffers = updatedRemainingOffers;
                }
                recommendations.addAll(response.recommendations);
                LOGGER.info("  {} offer result: {}[{} rec{}], {} offer{} remaining",
                        runServiceName,
                        response.result,
                        response.recommendations.size(), response.recommendations.size() == 1 ? "" : "s",
                        remainingOffers.size(), remainingOffers.size() == 1 ? "" : "s");

                switch (response.result) {
                case FINISHED:
                    // This client has completed running and can be switched to uninstall.
                    finishedRuns.add(runServiceName);
                    break;
                case UNINSTALLED:
                    // This client has completed uninstall and can be removed.
                    uninstalledRuns.add(runServiceName);
                    break;
                case NOT_READY:
                    // This client wasn't ready. Tell upstream to short-decline any remaining offers so that it can get
                    // another chance shortly.
                    anyClientsNotReady = true;
                    break;
                case PROCESSED:
                    // No-op, keep going.
                    break;
                }

                // If we run out of unusedOffers we still keep going with an empty list of offers.
                // This is done in case any of the clients depends on us to turn the crank periodically.
            }
        } finally {
            runManager.unlockRuns();
        }

        if (!finishedRuns.isEmpty()) {
            LOGGER.info("Starting uninstall for {} service{}: {} (from {} total services)",
                    finishedRuns.size(), finishedRuns.size() == 1 ? "" : "s", finishedRuns);
            // Trigger uninstalls. Grabs a lock internally, so we need to be unlocked when calling it here.
            runManager.uninstallRuns(finishedRuns);
        }

        if (!uninstalledRuns.isEmpty()) {
            // Note: It's possible that we can have a race where we attempt to remove the same run twice. This is fine.
            //       (Picture two near-simultaneous calls to offers(): Both send offers, both get FINISHED back, ...)
            int remainingRunCount = runManager.removeRuns(uninstalledRuns);
            noClients = remainingRunCount <= 0;

            // Just in case, avoid invoking the uninstall callback until we are in an unlocked state. This avoids
            // deadlock if the callback itself calls back into us for any reason. This also ensures that we aren't
            // blocking other operations (e.g. offer/status handling) while these callbacks are running.
            for (String run : uninstalledRuns) {
                uninstallCallback.uninstalled(run);
            }
        }

        if (noClients) {
            // We just removed the last client(s). Should the rest of the framework be torn down?
            if (uninstallPlan.isPresent()) {
                // Yes: We're uninstalling everything and all runs have been cleaned up. Tell the caller that they can
                // finish with final framework cleanup. After they've finished, they will invoke unregistered(), at
                // which point we can set our deploy plan to complete.
                return OfferResponse.uninstalled();
            } else {
                // No: We're just not actively running anything. Decline short until we have runs.
                return OfferResponse.notReady(recommendations);
            }
        } else if (anyClientsNotReady) {
            // One or more clients said they weren't ready, or we don't have any clients. Tell upstream to short-decline
            // the unused offers, but still perform any operations returned by the ready clients.
            return OfferResponse.notReady(recommendations);
        } else {
            // We have one or more clients and they were all able to process offers, so tell upstream to long-decline.
            return OfferResponse.processed(recommendations);
        }
    }

    /**
     * Maps the reserved resources in the provided unused offers according to the services that own them, then queries
     * those services directly to see what resources they consider unexpected.
     *
     * <p>This is an optimization which avoids querying services about unexpected resources that don't relate to them.
     * <p>In addition to reducing unnecessary queries, this also improves isolation between services. They only see
     * resources which relate to them.
     */
    @Override
    public UnexpectedResourcesResponse getUnexpectedResources(Collection<Protos.Offer> unusedOffers) {
        // Resources can be unexpected for any of the following reasons:
        // - (CASE 1) Resources which lack a service name (shouldn't happen in practice)
        // - (CASE 2) Resources with an unrecognized service name (old resources?)
        // - (CASE 3) Resources whose matching service returned them as unexpected (old/decommissioned resources?)

        // For each offer, the resources which should be unreserved.
        Map<Protos.OfferID, OfferResources> unexpectedResources = new HashMap<>();
        // For each service, the resources associated with that service, paired with their parent offer(s)
        // In other words: serviceName => offerId => offer + [resourcesForService]
        Map<String, Map<Protos.OfferID, OfferResources>> offersByService = new HashMap<>();

        // Map reserved resources (and their parent offers) to the service that they're assigned to.
        // We can then query those services with the subset of offers that belong to them.
        for (Protos.Offer offer : unusedOffers) {
            for (Protos.Resource resource : offer.getResourcesList()) {
                Optional<String> serviceName = ResourceUtils.getServiceName(resource);
                if (serviceName.isPresent()) {
                    // Found service name: Store resource against serviceName+offerId to be evaluated below.
                    getEntry(offersByService, serviceName.get(), offer).add(resource);
                } else if (ResourceUtils.getReservation(resource).isPresent()) {
                    // (CASE 1) Malformed: Reserved resource which is missing a service name.
                    // Send directly to the unexpected list.
                    getEntry(unexpectedResources, offer).add(resource);
                } else {
                    // Not a reserved resource. Ignore for cleanup purposes.
                }
            }
        }
        LOGGER.info("Sorted reserved resources from {} offer{} into {} services: {}",
                unusedOffers.size(),
                unusedOffers.size() == 1 ? "" : "s",
                offersByService.size(),
                offersByService.keySet());
        if (!unexpectedResources.isEmpty()) {
            LOGGER.warn("Encountered {} malformed resources to clean up: {}",
                    unexpectedResources.size(), unexpectedResources.values());
        }

        // Iterate over offersByService and find out if the services in question still want the resources.
        // Any unwanted resources then get added to unexpectedResources.
        boolean anyFailedClients = false;
        for (Map.Entry<String, Map<Protos.OfferID, OfferResources>> entry : offersByService.entrySet()) {
            String serviceName = entry.getKey();
            Collection<OfferResources> serviceOffers = entry.getValue().values();

            Optional<AbstractScheduler> run = runManager.getRun(serviceName);
            if (!run.isPresent()) {
                // (CASE 2) Old or invalid service name. Consider all resources for this service as unexpected.
                LOGGER.info("  {} cleanup result: unknown service, all resources unexpected", serviceName);
                for (OfferResources serviceOffer : serviceOffers) {
                    getEntry(unexpectedResources, serviceOffer.getOffer()).addAll(serviceOffer.getResources());
                }
            } else {
                // Construct offers containing (only) these resources and pass them to the service.
                // See which of the resources are now unexpected by the service.
                List<Protos.Offer> offersToSend = new ArrayList<>(serviceOffers.size());
                for (OfferResources serviceOfferResources : serviceOffers) {
                    offersToSend.add(serviceOfferResources.getOffer().toBuilder()
                            .clearResources()
                            .addAllResources(serviceOfferResources.getResources())
                            .build());
                }
                // (CASE 3) The service has returned the subset of these resources which are unexpected.
                // Add those to unexpectedResources.
                // Note: We're careful to only invoke this once per service, as the call is likely to be expensive.
                UnexpectedResourcesResponse response = run.get().getUnexpectedResources(offersToSend);
                LOGGER.info("  {} cleanup result: {} with {} unexpected resources in {} offer{}",
                        serviceName,
                        response.result,
                        response.offerResources.stream()
                                .collect(Collectors.summingInt(or -> or.getResources().size())),
                        response.offerResources.size(),
                        response.offerResources.size() == 1 ? "" : "s");
                switch (response.result) {
                case FAILED:
                    // We should be able to safely skip this service and proceed to the next one. For this round,
                    // the service just won't have anything added to unexpectedResources. We play it safe by telling
                    // upstream to do a short decline for the unprocessed resources.
                    anyFailedClients = true;
                    for (OfferResources unexpectedInOffer : response.offerResources) {
                        getEntry(unexpectedResources, unexpectedInOffer.getOffer())
                                .addAll(unexpectedInOffer.getResources());
                    }
                    break;
                case PROCESSED:
                    for (OfferResources unexpectedInOffer : response.offerResources) {
                        getEntry(unexpectedResources, unexpectedInOffer.getOffer())
                                .addAll(unexpectedInOffer.getResources());
                    }
                    break;
                }
            }
        }

        // Return the combined listing of unexpected resources across all services:
        return anyFailedClients
                ? UnexpectedResourcesResponse.failed(unexpectedResources.values())
                : UnexpectedResourcesResponse.processed(unexpectedResources.values());
    }

    /**
     * Maps the provided status to the service that owns its task, then queries that service with the status.
     *
     * <p>This is an optimization which avoids querying services about task statuses that don't relate to them.
     * <p>In addition to reducing unnecessary queries, this also improves isolation between services. They only see
     * task statuses which relate to them.
     */
    @Override
    public StatusResponse status(Protos.TaskStatus status) {
        Optional<String> serviceName;
        try {
            serviceName = CommonIdUtils.toServiceName(status.getTaskId());
        } catch (TaskException e) {
            serviceName = Optional.empty();
        }
        if (!serviceName.isPresent()) {
            // Bad task id.
            LOGGER.error("Received task status with malformed id '{}', unable to route to service: {}",
                    status.getTaskId().getValue(), TextFormat.shortDebugString(status));
            return StatusResponse.unknownTask();
        }

        Optional<AbstractScheduler> run = runManager.getRun(serviceName.get());
        if (!run.isPresent()) {
            // Unrecognized service. Status for old task?
            LOGGER.info("Received task status for unknown service {}: {}",
                    serviceName.get(), TextFormat.shortDebugString(status));
            return StatusResponse.unknownTask();
        } else {
            LOGGER.info("Received task status for service {}: {}={}",
                    serviceName.get(), status.getTaskId().getValue(), status.getState());
            return run.get().status(status);
        }
    }

    /**
     * Returns a set of queues-specific endpoints to be served by the scheduler. This effectively overrides the
     * underlying per-service endpoints with runs-aware versions.
     */
    @Override
    public Collection<Object> getHTTPEndpoints() {
        Collection<PlanManager> planManagers = uninstallPlan.isPresent()
                ? Collections.singletonList(DefaultPlanManager.createProceeding(uninstallPlan.get()))
                : Collections.emptyList(); // ... any plans to show when running normally?
        RunInfoProvider runInfoProvider = runManager.getRunInfoProvider();
        return Arrays.asList(
                new HealthResource(planManagers),
                new PlansResource(planManagers),
                new QueueResource(specStore, runManager, runGenerators, defaultSpecType),
                new RunArtifactResource(runInfoProvider),
                new RunConfigResource(runInfoProvider),
                new RunPlansResource(runInfoProvider),
                new RunPodResource(runInfoProvider),
                new RunStateResource(runInfoProvider, new StringPropertyDeserializer()));
    }

    /**
     * Finds the requested entry in the provided map, initializing the entry if needed.
     */
    private static OfferResources getEntry(
            Map<String, Map<Protos.OfferID, OfferResources>> map, String serviceName, Protos.Offer offer) {
        Map<Protos.OfferID, OfferResources> serviceOffers = map.get(serviceName);
        if (serviceOffers == null) {
            serviceOffers = new HashMap<>();
            map.put(serviceName, serviceOffers);
        }
        return getEntry(serviceOffers, offer);
    }

    /**
     * Finds the requested entry in the provided map, initializing the entry if needed.
     */
    private static OfferResources getEntry(Map<Protos.OfferID, OfferResources> map, Protos.Offer offer) {
        OfferResources currentValue = map.get(offer.getId());
        if (currentValue == null) {
            // Initialize entry
            currentValue = new OfferResources(offer);
            map.put(offer.getId(), currentValue);
        }
        return currentValue;
    }
}
