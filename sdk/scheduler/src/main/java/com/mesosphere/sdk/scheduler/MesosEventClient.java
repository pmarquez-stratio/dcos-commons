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
     * Response object to be returned by {@code offers()}.
     */
    public static class OfferResponse {

        /**
         * The outcome of this offers call.
         */
        public enum Result {
            /**
             * The client was not ready to process these offers. Come back later.
             */
            NOT_READY,

            /**
             * The client processed these offers and was not interested in them.
             */
            PROCESSED
        }

        /**
         * The result of the call. Delineates between "not ready" and "processed".
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
            return new OfferResponse(Result.NOT_READY, Collections.emptyList(), allOffers);
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
     * Response object to be returned by {@code status()}.
     */
    public static class StatusResponse {

        /**
         * The outcome of this status call.
         */
        public enum Result {
            /**
             * The task is not known to this service.
             */
            UNKNOWN_TASK,

            /**
             * The task status was processed.
             */
            PROCESSED
        }

        /**
         * The result of the call. Delineates between "unknown" and "processed".
         */
        public final Result result;

        public static StatusResponse unknownTask() {
            return new StatusResponse(Result.UNKNOWN_TASK);
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
     */
    public void register(boolean reRegistered);

    /**
     * Called when the framework has received offers from Mesos. The provided list may be empty.
     *
     * @return The list of offers which were NOT used by this client. For example, a no-op call would just return the
     *         list of {@code offers} that were provided.
     */
    public OfferResponse offers(List<Protos.Offer> offers);

    /**
     * Returns a list of known/expected resources owned by this client. Any reserved resources not on this list will be
     * unreserved/destroyed.
     */
    public Collection<Protos.Resource> getExpectedResources();

    /**
     * Called when resources have been cleaned. Mainly useful for uninstall, when cleaned resources can be marked as
     * completed.
     *
     * @param recommendations A list of zero or more {@code UninstallRecommendation}s
     */
    public void cleaned(Collection<OfferRecommendation> recommendations);

    /**
     * Called when the framework has received a task status update from Mesos.
     */
    public StatusResponse status(Protos.TaskStatus status);

    /**
     * Returns any HTTP resources to be served by this instance.
     */
    public Collection<Object> getHTTPEndpoints();
}
