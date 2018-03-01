package com.mesosphere.sdk.offer;

import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.TextFormat;

import java.util.*;

/**
 * The Resource Cleaner provides recommended operations for cleaning up unexpected Reserved resources and persistent
 * volumes.
 */
public class ResourceCleaner {
    private static final Logger LOGGER = LoggerFactory.getLogger(ResourceCleaner.class);

    private final Collection<Protos.Resource> expectedResources;

    /**
     * Creates a new instance which cleans up resources which aren't in the provided list.
     */
    public ResourceCleaner(Collection<Protos.Resource> expectedResources) {
        this.expectedResources = expectedResources;
    }

    /**
     * Returns a list of operations which should be performed, given the provided list of Offers
     * from Mesos. The returned operations MUST be performed in the order in which they are
     * provided.
     */
    public List<OfferRecommendation> evaluate(List<Protos.Offer> offers) {
        // ORDERING IS IMPORTANT:
        //    The resource lifecycle is RESERVE -> CREATE -> DESTROY -> UNRESERVE
        //    Therefore we *must* put any DESTROY calls before any UNRESERVE calls
        List<OfferRecommendation> recommendations = new ArrayList<>();

        // First, find any persistent volumes to be DESTROYed
        for (Protos.Offer offer : offers) {
            for (Protos.Resource persistentVolume : getPersistentVolumesToBeDestroyed(offer)) {
                recommendations.add(new DestroyOfferRecommendation(offer, persistentVolume));
            }
        }

        // Then, find any unexpected persistent volumes AND resource reservations which should (both) be UNRESERVEd
        for (Protos.Offer offer : offers) {
            for (Protos.Resource reservedResource : getReservedResourcesToBeUnreserved(offer)) {
                recommendations.add(new UnreserveOfferRecommendation(offer, reservedResource));
            }
        }

        return recommendations;
    }

    /**
     * Examines the {@link Offer} to determine which {@link Resource}s should be unreserved.
     *
     * @param offer The {@link Offer} containing the {@link Resource}s.
     * @return A {@link Collection} of {@link Resource}s that should be unreserved.
     */
    private Collection<? extends Protos.Resource> getReservedResourcesToBeUnreserved(Protos.Offer offer) {
        return selectUnexpectedResources(getReservedResourceIds(expectedResources), getReservedResourcesById(offer));
    }

    /**
     * Examines the {@link Offer} to determine which volume {@link Resource}s should be destroyed.
     *
     * @param offer The {@link Offer} containing the persistent volume {@link Resource}s.
     * @return A {@link Collection} of {@link Resource}s that should be destroyed.
     */
    private Collection<? extends Protos.Resource> getPersistentVolumesToBeDestroyed(Protos.Offer offer) {
        return selectUnexpectedResources(getPersistentVolumeIds(expectedResources), getPersistentVolumesById(offer));
    }

    /**
     * Returns a list of resources from {@code resourcesById} whose ids are not present in
     * {@code expectedIds}.
     */
    private static Collection<Protos.Resource> selectUnexpectedResources(
            Set<String> expectedIds, Map<String, Protos.Resource> resourcesById) {
        List<Protos.Resource> unexpectedResources = new ArrayList<>();

        for (Map.Entry<String, Protos.Resource> entry : resourcesById.entrySet()) {
            if (!expectedIds.contains(entry.getKey())) {
                LOGGER.info("Resource to be unreserved: {}", TextFormat.shortDebugString(entry.getValue()));
                unexpectedResources.add(entry.getValue());
            }
        }
        return unexpectedResources;
    }

    /**
     * Returns the resource ids for all {@code resources} which represent persistent volumes, or
     * an empty list if no persistent volume resources were found.
     */
    private static Set<String> getPersistentVolumeIds(Collection<Protos.Resource> resources) {
        Set<String> persistenceIds = new HashSet<>();
        for (Protos.Resource resource : resources) {
            if (resource.hasDisk() && resource.getDisk().hasPersistence()) {
                persistenceIds.add(resource.getDisk().getPersistence().getId());
            }
        }
        return persistenceIds;
    }

    /**
     * Returns the resource ids for all {@code resources} which represent reserved resources, or
     * an empty list if no reserved resources were found.
     */
    private static Set<String> getReservedResourceIds(Collection<Protos.Resource> resources) {
        Set<String> resourceIds = new HashSet<>();
        resourceIds.addAll(ResourceUtils.getResourceIds(resources));
        return resourceIds;
    }

    /**
     * Returns an ID -> Resource mapping of all disk resources listed in the provided {@link Offer},
     * or an empty list of no disk resources are found.
     * @param offer The Offer being deconstructed.
     * @return The map of resources from the {@link Offer}
     */
    private static Map<String, Protos.Resource> getPersistentVolumesById(Protos.Offer offer) {
        Map<String, Protos.Resource> volumes = new HashMap<>();
        for (Protos.Resource resource : offer.getResourcesList()) {
            if (resource.hasDisk() && resource.getDisk().hasPersistence()) {
                volumes.put(resource.getDisk().getPersistence().getId(), resource);
            }
        }
        return volumes;
    }

    /**
     * Returns an ID -> Resource mapping of all reservation resources listed in the provided
     * {@link Offer}, or an empty list if no reservation resources are found.
     */
    private static Map<String, Protos.Resource> getReservedResourcesById(Protos.Offer offer) {
        Map<String, Protos.Resource> reservedResources = new HashMap<>();
        for (Protos.Resource resource : offer.getResourcesList()) {
            Optional<String> resourceId = ResourceUtils.getResourceId(resource);
            if (resourceId.isPresent()) {
                reservedResources.put(resourceId.get(), resource);
            }
        }
        return reservedResources;
    }
}
