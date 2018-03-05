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
import com.mesosphere.sdk.scheduler.uninstall.UninstallScheduler;

/**
 * Mesos client which wraps running jobs, routing Mesos events to the appropriate jobs.
 */
public class JobsEventClient implements MesosEventClient {

    private static final Logger LOGGER = LoggingUtils.getLogger(JobsEventClient.class);

    private final boolean isUninstalling;
    private final DefaultJobInfoProvider jobInfoProvider;

    public JobsEventClient(SchedulerConfig schedulerConfig) {
        this.isUninstalling = schedulerConfig.isUninstallEnabled();
        this.jobInfoProvider = new DefaultJobInfoProvider();
    }

    /**
     * Adds a job which is mapped for the specified name.
     *
     * @param job the client to add
     * @return {@code this}
     * @throws IllegalArgumentException if the name is already present
     */
    public JobsEventClient putJob(ServiceScheduler job) {
        Map<String, ServiceScheduler> jobs = jobInfoProvider.lockRW();
        LOGGER.info("Adding service: {} (now {} services)", job.getName(), jobs.size() + 1);
        try {
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
     * is already uninstalling.
     *
     * @param name the name of the job to be uninstalled
     * @return {@code this}
     * @throws IllegalArgumentException if the name does not exist
     */
    public JobsEventClient uninstallJob(String name) {
        // TODO(nickbp): UNINSTALL Need to persist the fact that this job is uninstalling and automatically resume
        // uninstalling it if the scheduler gets restarted. Flow:
        // - UninstallScheduler sets the bit within the storage namespace when uninstall starts (moved from always-root)
        // - SchedulerBuilder checks for the bit (in addition to current SchedulerConfig bit)
        // - Assuming that upstream consistently uses SchedulerBuilder, we should get UninstallSchedulers added
        //   automatically. BUT we need a way to tell upstream when they should or shouldn't be creating the schedulers.
        //   Maybe have a service that just automatically rebuilds schedulers based off of zk/config storage?
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
            // Convert the DefaultScheduler to an UninstallScheduler, which will then perform the uninstall. When it has
            // completed, it will return FINISHED to its next offers() call, at which point we will remove it.
            jobs.put(name, ((DefaultScheduler) currentJob).toUninstallScheduler());
            return this;
        } finally {
            jobInfoProvider.unlockRW();
        }
    }

    @Override
    public void register(boolean reRegistered) {
        Collection<ServiceScheduler> jobs = jobInfoProvider.lockAllR();
        LOGGER.info("Notifying {} services of {}",
                jobs.size(), reRegistered ? "re-registration" : "initial registration");
        try {
            jobs.stream().forEach(c -> c.register(reRegistered));
        } finally {
            jobInfoProvider.unlockR();
        }
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
                if (isUninstalling) {
                    // We're uninstalling everything and all jobs have been cleaned up. Tell the caller that they can
                    // finish with final framework cleanup.
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
                // TODO(nickbp): this will be ALWAYS HEALTHY... Options:
                // - add some plans from a parent queue scheduler? (if we should have one)
                // - implement a custom HealthResource which isn't plan-based?
                // this likely ties into uninstall mode. need to tell cosmos when we're done uninstalling
                new HealthResource(Collections.emptyList()),
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
