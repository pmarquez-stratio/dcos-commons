package com.mesosphere.sdk.scheduler;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.mesos.Protos;

import com.mesosphere.sdk.offer.OfferRecommendation;

/**
 * Accepts events received from Mesos.
 */
public interface MesosEventClient {

    /**
     * The outcome value to be included in a response object.
     */
    public enum Result {
        /**
         * The client failed or was not ready to process the request. Short-decline and come back later.
         */
        FAILED,

        /**
         * The client processed the request successfully.
         */
        PROCESSED
    }

    /**
     * Response object to be returned by a call to {@link MesosEventClient#offers(List)}.
     */
    public static class OfferResponse {

        /**
         * The result of the call. Delineates between "not ready" (decline short) and "processed" (decline long).
         */
        public final Result result;

        /**
         * The operations to be performed using the provided offers.
         */
        public final List<OfferRecommendation> recommendations;

        /**
         * The offers which were not used to perform an operation.
         */
        public final List<Protos.Offer> unusedOffers;

        public static OfferResponse notReady(List<Protos.Offer> allOffers) {
            return new OfferResponse(Result.FAILED, Collections.emptyList(), allOffers);
        }

        public static OfferResponse processed(
                List<OfferRecommendation> recommendations, List<Protos.Offer> unusedOffers) {
            return new OfferResponse(Result.PROCESSED, recommendations, unusedOffers);
        }

        private OfferResponse(
                Result result, List<OfferRecommendation> recommendations, List<Protos.Offer> unusedOffers) {
            this.result = result;
            this.recommendations = recommendations;
            this.unusedOffers = unusedOffers;
        }
    }
    /**
     * Response object to be returned by a call to {@link MesosEventClient##getUnexpectedResources(List)}.
     */
    public static class UnexpectedResourcesResponse {

        /**
         * The result of the call. Delineates between "failed" (decline short) and "processed" (decline long).
         */
        public final Result result;

        /**
         * The resources which are unexpected, paired with their parent offers.
         */
        public final Collection<OfferResources> offerResources;

        public static UnexpectedResourcesResponse failed(Collection<OfferResources> offerResources) {
            return new UnexpectedResourcesResponse(Result.FAILED, offerResources);
        }

        public static UnexpectedResourcesResponse processed(Collection<OfferResources> offerResources) {
            return new UnexpectedResourcesResponse(Result.PROCESSED, offerResources);
        }

        private UnexpectedResourcesResponse(Result result, Collection<OfferResources> offerResources) {
            this.result = result;
            this.offerResources = offerResources;
        }
    }

    /**
     * Response object to be returned by a call to {@link MesosEventClient#status(org.apache.mesos.Protos.TaskStatus)}.
     */
    public static class StatusResponse {

        /**
         * The result of the call. Delineates between "unknown" and "processed".
         */
        public final Result result;

        public static StatusResponse unknownTask() {
            return new StatusResponse(Result.FAILED);
        }

        public static StatusResponse processed() {
            return new StatusResponse(Result.PROCESSED);
        }

        private StatusResponse(Result result) {
            this.result = result;
        }
    }

    /**
     * Called when the framework has registered (or re-registered) with Mesos.
     *
     * @param reRegistered Whether this is an initial registration ({@code false}) or a re-registration ({@code true})
     */
    public void register(boolean reRegistered);

    /**
     * Called when the framework has received offers from Mesos. The provided list may be empty.
     *
     * @param offers A list of offers which may be used in offer evaluation
     * @return The response containing a list of operations to be performed against the offers, as well as a list of
     *         offers which were left unused. See {@link OfferResponse}
     */
    public OfferResponse offers(List<Protos.Offer> offers);

    /**
     * Returns a list of resources from the provided list of offers which are not recognized by this client. The
     * returned unexpected resources will immediately be unreserved/destroyed.
     *
     * @param unusedOffers The list of offers which were unclaimed in a prior call to {@link #offers(List)}
     * @return A subset of the provided offers paired with their resources to be unreserved/destroyed. May be paired
     *         with an error state if remaining unused offers should be declined-short rather than declined-long.
     */
    public UnexpectedResourcesResponse getUnexpectedResources(List<Protos.Offer> unusedOffers);

    /**
     * Called when the framework has received a task status update from Mesos.
     *
     * @param status The status message describing the new state of a task
     * @return The response which describes whether the status was successfully processed
     */
    public StatusResponse status(Protos.TaskStatus status);

    /**
     * Returns any HTTP resources to be served on behalf of this instance.
     *
     * @return A list of annotated resource objects to be served by Jetty
     */
    public Collection<Object> getHTTPEndpoints();
}
