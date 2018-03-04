package com.mesosphere.sdk.queues.scheduler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mesosphere.sdk.http.endpoints.*;
import com.mesosphere.sdk.http.types.StringPropertyDeserializer;
import com.mesosphere.sdk.offer.CommonIdUtils;
import com.mesosphere.sdk.offer.OfferRecommendation;
import com.mesosphere.sdk.offer.ResourceUtils;
import com.mesosphere.sdk.offer.TaskException;
import com.mesosphere.sdk.queues.http.endpoints.*;
import com.mesosphere.sdk.scheduler.MesosEventClient;
import com.mesosphere.sdk.scheduler.OfferResources;
import com.mesosphere.sdk.scheduler.ServiceScheduler;

/**
 * Mesos client which wraps running jobs, routing Mesos events to the appropriate jobs.
 */
public class JobsEventClient implements MesosEventClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobsEventClient.class);

    private final DefaultJobInfoProvider jobInfoProvider;

    public JobsEventClient() {
        jobInfoProvider = new DefaultJobInfoProvider();
    }

    /**
     * Adds a job which is mapped for the specified name.
     *
     * @param name the unique name of the client
     * @param job the client to add
     * @return {@code this}
     * @throws IllegalArgumentException if the name is already present
     */
    public JobsEventClient putJob(String name, ServiceScheduler job) {
        LOGGER.info("putJob {}", name);
        Map<String, ServiceScheduler> jobs = jobInfoProvider.lockRW();
        try {
            ServiceScheduler previousJob = jobs.put(name, job);
            if (previousJob != null) {
                // Put the old client back before throwing...
                jobs.put(name, previousJob);
                throw new IllegalArgumentException("Service named '" + name + "' is already present");
            }
            return this;
        } finally {
            jobInfoProvider.unlockRW();
        }
    }

    /**
     * Removes a client mapping which was previously added using the provided name.
     *
     * @param name the name of the client to remove
     * @return the removed client, or {@code null} if no client with that name was found
     */
    public MesosEventClient removeJob(String name) {
        LOGGER.info("removeJob {}", name);
        Map<String, ServiceScheduler> jobs = jobInfoProvider.lockRW();
        try {
            return jobs.remove(name);
        } finally {
            jobInfoProvider.unlockRW();
        }
    }

    @Override
    public void register(boolean reRegistered) {
        LOGGER.info("register reRegistered={}", reRegistered);
        Collection<ServiceScheduler> jobs = jobInfoProvider.lockAllR();
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
    public OfferResponse offers(List<Protos.Offer> offers) {
        LOGGER.info("offers={}", offers.size());
        boolean anyNotReady = false;

        List<OfferRecommendation> recommendations = new ArrayList<>();
        List<Protos.Offer> unusedOffers = new ArrayList<>();
        unusedOffers.addAll(offers);

        Collection<ServiceScheduler> jobs = jobInfoProvider.lockAllR();
        try {
            if (jobs.isEmpty()) {
                // If we don't have any clients, then WE aren't ready. Decline short.
                anyNotReady = true;
            }
            for (MesosEventClient job : jobs) {
                OfferResponse response = job.offers(unusedOffers);
                // Create a new list with unused offers. Avoid clearing in-place, in case response is the original list.
                unusedOffers = new ArrayList<>();
                unusedOffers.addAll(response.unusedOffers);
                recommendations.addAll(response.recommendations);
                LOGGER.info("  response={}: recommendations={} unusedOffers={}",
                        response.result, response.recommendations.size(), response.unusedOffers.size());

                if (response.result == MesosEventClient.Result.FAILED) {
                    // This client wasn't ready. Decline short.
                    anyNotReady = true;
                }

                // If we run out of unusedOffers we still keep going with an empty list of offers.
                // This is done in case any of the clients depends on us to turn the crank periodically.
            }
        } finally {
            jobInfoProvider.unlockR();
        }

        LOGGER.info("  anyNotReady={}", anyNotReady);
        return anyNotReady
                ? OfferResponse.notReady(unusedOffers)
                : OfferResponse.processed(recommendations, unusedOffers);
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
    public UnexpectedResourcesResponse getUnexpectedResources(List<Protos.Offer> unusedOffers) {
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
                    // Not a reserved. Ignore.
                }
            }
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
                    switch (response.result) {
                    case FAILED:
                        LOGGER.error("Failed to get unexpected resources from job {}, leaving it as-is", serviceName);
                        // We should be able to safely skip this service and proceed to the next one. For this round,
                        // the service just won't have anything added to unexpectedResources. We play it safe tell
                        // upstream to do a short decline for the unprocessed resources.
                        anyFailedClients = true;
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
            return StatusResponse.unknownTask();
        }

        ServiceScheduler job = jobInfoProvider.lockJobR(serviceName.get());
        try {
            if (job == null) {
                // Unrecognized service. Old task?
                return StatusResponse.unknownTask();
            }
            return job.status(status);
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
