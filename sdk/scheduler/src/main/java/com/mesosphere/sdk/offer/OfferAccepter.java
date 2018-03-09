package com.mesosphere.sdk.offer;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.TextFormat;
import com.mesosphere.sdk.framework.Driver;

import org.apache.commons.collections.CollectionUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.slf4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

/**
 * The OfferAccepter extracts the Mesos Operations encapsulated by the OfferRecommendation and accepts Offers with those
 * Operations.
 */
public class OfferAccepter {
    /**
     * Tell Mesos to consider unused resources as refused for 1 second.
     */
    private static final Protos.Filters FILTERS = Protos.Filters.newBuilder().setRefuseSeconds(1).build();

    private final Logger logger = LoggingUtils.getLogger(getClass());

    public void accept(List<OfferRecommendation> allRecommendations) {
        if (CollectionUtils.isEmpty(allRecommendations)) {
            logger.info("No recommendations, nothing to do");
            return;
        }

        Optional<SchedulerDriver> driver = Driver.getDriver();
        if (!driver.isPresent()) {
            throw new IllegalStateException("No driver present for accepting offers.  This should never happen.");
        }

        // Group recommendations by agent: Mesos requires that acceptOffers() only applies to a single agent at a time.
        // Note that ORDERING IS IMPORTANT:
        //    The resource lifecycle is RESERVE -> CREATE -> DESTROY -> UNRESERVE
        //    Therefore we must preserve ordering within each per-agent set of operations.
        final Map<String, List<OfferRecommendation>> recsByAgent = groupByAgent(allRecommendations);
        for (Map.Entry<String, List<OfferRecommendation>> agentRecs : recsByAgent.entrySet()) {
            List<Protos.Offer.Operation> operations = agentRecs.getValue().stream()
                    .map(rec -> rec.getOperation())
                    .collect(Collectors.toList());

            logger.info("Sending {} operations for agent {}:", operations.size(), agentRecs.getKey());
            for (Protos.Offer.Operation op : operations) {
                logger.info("  {}", TextFormat.shortDebugString(op));
            }

            driver.get().acceptOffers(
                    agentRecs.getValue().stream()
                            .map(rec -> rec.getOffer().getId())
                            .collect(Collectors.toSet()),
                    operations,
                    FILTERS);
        }
    }

    /**
     * Groups recommendations by agent, while preserving their existing order.
     */
    @VisibleForTesting
    protected static Map<String, List<OfferRecommendation>> groupByAgent(List<OfferRecommendation> recommendations) {
        // Use TreeMap for consistent ordering. Not required but simplifies testing, and nice to have consistent output.
        final Map<String, List<OfferRecommendation>> recommendationsByAgent = new TreeMap<>();
        for (OfferRecommendation recommendation : recommendations) {
            final String agentId = recommendation.getOffer().getSlaveId().getValue();
            List<OfferRecommendation> agentRecommendations = recommendationsByAgent.get(agentId);
            if (agentRecommendations == null) {
                agentRecommendations = new ArrayList<>();
                recommendationsByAgent.put(agentId, agentRecommendations);
            }
            agentRecommendations.add(recommendation);
        }
        return recommendationsByAgent;
    }
}
