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
import com.mesosphere.sdk.queues.http.endpoints.*;
import com.mesosphere.sdk.scheduler.DefaultScheduler;
import com.mesosphere.sdk.scheduler.MesosEventClient;
import com.mesosphere.sdk.scheduler.OfferResources;
import com.mesosphere.sdk.scheduler.SchedulerConfig;
import com.mesosphere.sdk.scheduler.ServiceScheduler;
import com.mesosphere.sdk.scheduler.plan.DefaultPhase;
import com.mesosphere.sdk.scheduler.plan.DefaultPlan;
import com.mesosphere.sdk.scheduler.plan.DefaultPlanManager;
import com.mesosphere.sdk.scheduler.plan.Plan;
import com.mesosphere.sdk.scheduler.plan.strategy.SerialStrategy;
import com.mesosphere.sdk.scheduler.uninstall.DeregisterStep;
import com.mesosphere.sdk.scheduler.uninstall.UninstallScheduler;

/**
 * Mesos client which wraps running jobs, routing Mesos events to the appropriate jobs.
 */
public class JobsEventClient implements MesosEventClient {

    /**
     * Interface for notifying the caller that added jobs have completed uninstall.
     */
    public interface UninstallCallback {
        /**
         * Invoked when a given job has completed its uninstall as triggered by
         * {@link JobsEventClient#uninstallJob(String)}. After this has been called, re-adding the job to the
         * {@link JobsEventClient} will result in launching a new instance from scratch.
         *
         * @param jobName the name of the job which has completed uninstall and which has been freed from the client
         */
        void uninstalled(String jobName);
    }

    private static final Logger LOGGER = LoggingUtils.getLogger(JobsEventClient.class);

    private final DeregisterStep deregisterStep;
    private final Optional<Plan> uninstallPlan;
    private final DefaultJobInfoProvider jobInfoProvider;
    private final UninstallCallback uninstallCallback;

    public JobsEventClient(SchedulerConfig schedulerConfig, UninstallCallback uninstallCallback) {
        this.jobInfoProvider = new DefaultJobInfoProvider();
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

    /**
     * Adds a job which is mapped for the specified name. Note: If the job was marked for uninstall via
     * {@link #uninstallJob(String)}, it should continue to be added across scheduler restarts in order for uninstall to
     * complete. It should only be omitted after the uninstall callback has been invoked for it.
     *
     * @param job the client to add
     * @return {@code this}
     * @throws IllegalArgumentException if the job name is already present
     */
    public JobsEventClient putJob(ServiceScheduler job) {
        Map<String, ServiceScheduler> jobs = jobInfoProvider.lockRW();
        LOGGER.info("Adding service: {} (now {} services)", job.getName(), jobs.size() + 1);
        try {
            // NOTE: If the job is uninstalling, it should already be passed to us as an UninstallScheduler.
            // See SchedulerBuilder.
            ServiceScheduler previousJob = jobs.put(job.getName(), job);
            if (previousJob != null) {
                // Put the old client back before throwing...
                jobs.put(job.getName(), previousJob);
                throw new IllegalArgumentException(
                        String.format("Service named '%s' is already present", job.getName()));
            }
            return this;
        } finally {
            jobInfoProvider.unlockRW();
        }
    }

    /**
     * Triggers an uninstall for a job, removing it from the list of jobs when it has finished. Does nothing if the job
     * is already uninstalling. If the scheduler process is restarted, the job must be added again via {@link putJob},
     * at which point it will automatically resume uninstalling.
     *
     * @param name the name of the job to be uninstalled
     * @return {@code this}
     * @throws IllegalArgumentException if the name does not exist
     */
    public JobsEventClient uninstallJob(String name) {
        // UNINSTALL FLOW:
        // 1. uninstallJob("foo") is called. This converts the job to an UninstallScheduler.
        // 2. UninstallScheduler internally flags its StateStore with an uninstall bit if one is not already present.
        // 3. The UninstallScheduler proceeds to clean up the service.
        // 4. In the event of a scheduler process restart during cleanup:
        //   a. Upstream builds a new foo using SchedulerBuilder, which internally finds the uninstall bit and returns a
        //      new UninstallScheduler
        //   b. putJob(foo) is called with the UninstallScheduler
        //   c. The UninstallScheduler resumes cleanup from where it left off...
        // 5. Sometime after the UninstallScheduler finishes cleanup, it returns FINISHED in response to offers.
        // 6. We remove the job and invoke uninstallCallback.uninstalled(), telling upstream that it's gone. If upstream
        //    invokes putJob(foo) again at this point, the job will be relaunched from scratch because the uninstall bit
        //    in ZK will have been cleared.
        Map<String, ServiceScheduler> jobs = jobInfoProvider.lockRW();
        LOGGER.info("Marking service as uninstalling: {} (out of {} services)", name, jobs.size());
        try {
            ServiceScheduler currentJob = jobs.get(name);
            if (currentJob == null) {
                throw new IllegalArgumentException(String.format(
                        "Service '%s' does not exist: nothing to uninstall", name));
            }
            if (currentJob instanceof UninstallScheduler) {
                // Already uninstalling
                LOGGER.warn("Told to uninstall service '{}', but it is already uninstalling", name);
                return this;
            }

            // Convert the DefaultScheduler to an UninstallScheduler. It will automatically flag itself with an
            // uninstall bit in its state store and then proceed with the uninstall. When the uninstall has completed,
            // it will return FINISHED to its next offers() call, at which point we will remove it. If the scheduler
            // process is restarted before uninstall has completed, the caller should have added it back via putJob().
            // When it's added back, it should be have already been converted to an UninstallScheduler. See
            // SchedulerBuilder.
            jobs.put(name, ((DefaultScheduler) currentJob).toUninstallScheduler());

            return this;
        } finally {
            jobInfoProvider.unlockRW();
        }
    }

    @Override
    public void registered(boolean reRegistered) {
        Collection<ServiceScheduler> jobs = jobInfoProvider.lockAllR();
        LOGGER.info("Notifying {} services of {}",
                jobs.size(), reRegistered ? "re-registration" : "initial registration");
        try {
            jobs.stream().forEach(c -> c.registered(reRegistered));
        } finally {
            jobInfoProvider.unlockR();
        }
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
        boolean anyNotReady = false;

        List<OfferRecommendation> recommendations = new ArrayList<>();
        List<Protos.Offer> remainingOffers = new ArrayList<>();
        remainingOffers.addAll(offers);

        Collection<String> jobsToRemove = new ArrayList<>();

        Collection<ServiceScheduler> jobs = jobInfoProvider.lockAllR();
        LOGGER.info("Sending {} offer{} to {} service{}:",
                offers.size(), offers.size() == 1 ? "" : "s",
                jobs.size(), jobs.size() == 1 ? "" : "s");
        try {
            if (jobs.isEmpty()) {
                if (uninstallPlan.isPresent()) {
                    // We're uninstalling everything and all jobs have been cleaned up. Tell the caller that they can
                    // finish with final framework cleanup. After they've finished, they will invoke unregistered(), at
                    // which point we can set our deploy plan to complete.
                    return OfferResponse.finished();
                } else {
                    // If we don't have any clients, then WE aren't ready. Decline short.
                    anyNotReady = true;
                }
            }
            for (ServiceScheduler job : jobs) {
                OfferResponse response = job.offers(remainingOffers);
                if (!remainingOffers.isEmpty() && !response.recommendations.isEmpty()) {
                    // Some offers were consumed. Update what remains to offer to the next job.
                    List<Protos.Offer> updatedRemainingOffers =
                            OfferUtils.filterOutAcceptedOffers(remainingOffers, response.recommendations);
                    remainingOffers = updatedRemainingOffers;
                }
                recommendations.addAll(response.recommendations);
                LOGGER.info("  {} offer result: {}[{} rec{}], {} offer{} remaining",
                        job.getName(),
                        response.result,
                        response.recommendations.size(), response.recommendations.size() == 1 ? "" : "s",
                        remainingOffers.size(), remainingOffers.size() == 1 ? "" : "s");

                switch (response.result) {
                case FINISHED:
                    // This client has completed an uninstall operation.
                    jobsToRemove.add(job.getName());
                    break;
                case NOT_READY:
                    // This client wasn't ready. Tell upstream to short-decline any remaining offers.
                    anyNotReady = true;
                    break;
                case PROCESSED:
                    // No-op, keep going.
                    break;
                }

                // If we run out of unusedOffers we still keep going with an empty list of offers.
                // This is done in case any of the clients depends on us to turn the crank periodically.
            }
        } finally {
            jobInfoProvider.unlockR();
        }

        if (!jobsToRemove.isEmpty()) {
            // Note: It's possible that we can have a race where we attempt to remove the same job twice. This is fine.
            //       (Picture two near-simultaneous calls to offers(): Both send offers, both get FINISHED back, ...)
            Map<String, ServiceScheduler> jobsMap = jobInfoProvider.lockRW();
            LOGGER.info("Removing {} uninstalled service{}: {} (from {} total services)",
                    jobsToRemove.size(), jobsToRemove.size() == 1 ? "" : "s", jobsToRemove, jobsMap.size());
            try {
                for (String job : jobsToRemove) {
                    jobsMap.remove(job);
                }
            } finally {
                jobInfoProvider.unlockRW();
            }

            // Just in case, avoid invoking the uninstall callback until we are in an unlocked state. This avoids
            // deadlock if the callback itself calls back into us for any reason. This also ensures that we aren't
            // blocking other operations (e.g. offer/status handling) while these callbacks are running.
            for (String job : jobsToRemove) {
                uninstallCallback.uninstalled(job);
            }
        }

        return anyNotReady
                // Return the subset of recommendations that could still be performed, but tell upstream to
                // short-decline the unused offers:
                ? OfferResponse.notReady(recommendations)
                // All clients were able to process offers, so tell upstream to long-decline the unused offers:
                : OfferResponse.processed(recommendations);
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

            ServiceScheduler job = jobInfoProvider.lockJobR(serviceName);
            try {
                if (job == null) {
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
                    UnexpectedResourcesResponse response = job.getUnexpectedResources(offersToSend);
                    LOGGER.info("  {} cleanup result: {} with {} unexpected resources in {} offers",
                            serviceName,
                            response.result,
                            response.offerResources.stream()
                                    .collect(Collectors.summingInt(or -> or.getResources().size())),
                            response.offerResources.size());
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
            } finally {
                jobInfoProvider.unlockR();
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

        ServiceScheduler job = jobInfoProvider.lockJobR(serviceName.get());
        try {
            if (job == null) {
                // Unrecognized service. Status for old task?
                LOGGER.info("Received task status for unknown service {}: {}",
                        serviceName.get(), TextFormat.shortDebugString(status));
                return StatusResponse.unknownTask();
            } else {
                LOGGER.info("Received task status for service {}: {}={}",
                        serviceName.get(), status.getTaskId().getValue(), status.getState());
                return job.status(status);
            }
        } finally {
            jobInfoProvider.unlockR();
        }
    }

    /**
     * Returns a set of jobs-specific endpoints to be served by the scheduler.
     */
    @Override
    public Collection<Object> getHTTPEndpoints() {
        return Arrays.asList(
                // Note: In default deployment, this endpoint will be ALWAYS HEALTHY... Options:
                // - Add a deploy plan for the parent queue, if/when one exists?
                // - Implement a custom HealthResource which isn't plan-based?
                new HealthResource(uninstallPlan.isPresent()
                        ? Collections.singletonList(DefaultPlanManager.createProceeding(uninstallPlan.get()))
                        : Collections.emptyList()),
                new JobsResource(jobInfoProvider),
                new JobsArtifactResource(jobInfoProvider),
                new JobsConfigResource(jobInfoProvider),
                new JobsPlansResource(jobInfoProvider),
                new JobsPodResource(jobInfoProvider),
                new JobsStateResource(jobInfoProvider, new StringPropertyDeserializer()));
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
        OfferResources serviceOffer = serviceOffers.get(offer.getId());
        if (serviceOffer == null) {
            serviceOffer = new OfferResources(offer);
            serviceOffers.put(offer.getId(), serviceOffer);
        }
        return serviceOffer;
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
