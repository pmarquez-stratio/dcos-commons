package com.mesosphere.sdk.offer;

import com.mesosphere.sdk.testutils.*;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Offer.Operation;
import org.apache.mesos.Protos.Resource;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ResourceCleanerTest extends DefaultCapabilitiesTestSuite {

    private static final String EXPECTED_RESOURCE_1_ID = "expected-resource-id-1";
    private static final String EXPECTED_RESOURCE_2_ID = "expected-volume-id-2";
    private static final String UNEXPECTED_RESOURCE_1_ID = "unexpected-volume-id-1";
    private static final String UNEXPECTED_RESOURCE_2_ID = "unexpected-resource-id-2";
    private static final String UNEXPECTED_RESOURCE_3_ID = "unexpected-volume-id-3";

    private static final Resource EXPECTED_RESOURCE_1 =
            ResourceTestUtils.getReservedPorts(123, 234, EXPECTED_RESOURCE_1_ID);
    private static final Resource EXPECTED_RESOURCE_2 =
            ResourceTestUtils.getReservedRootVolume(999.0, EXPECTED_RESOURCE_2_ID, EXPECTED_RESOURCE_2_ID);

    private static final Resource UNEXPECTED_RESOURCE_1 =
            ResourceTestUtils.getReservedRootVolume(1000.0, UNEXPECTED_RESOURCE_1_ID, UNEXPECTED_RESOURCE_1_ID);
    private static final Resource UNEXPECTED_RESOURCE_2 =
            ResourceTestUtils.getReservedCpus(1.0, UNEXPECTED_RESOURCE_2_ID);
    private static final Resource UNEXPECTED_RESOURCE_3 =
            ResourceTestUtils.getReservedRootVolume(1001.0, UNEXPECTED_RESOURCE_3_ID, UNEXPECTED_RESOURCE_3_ID);

    private final ResourceCleaner emptyCleaner = new ResourceCleaner(Collections.emptyList());
    private final ResourceCleaner populatedCleaner =
            new ResourceCleaner(Arrays.asList(EXPECTED_RESOURCE_1, EXPECTED_RESOURCE_2));
    private final List<ResourceCleaner> allCleaners = Arrays.asList(emptyCleaner, populatedCleaner);

    @Test
    public void testNoOffers() {
        for (ResourceCleaner cleaner : allCleaners) {
            List<OfferRecommendation> recommendations = cleaner.evaluate(Collections.emptyList());

            assertNotNull(recommendations);
            assertEquals(Collections.emptyList(), recommendations);
        }
    }

    @Test
    public void testUnexpectedVolume() {
        List<Offer> offers = OfferTestUtils.getOffers(UNEXPECTED_RESOURCE_1);

        for (ResourceCleaner cleaner : allCleaners) {
            List<OfferRecommendation> recommendations = cleaner.evaluate(offers);

            assertEquals("Got: " + recommendations, 2, recommendations.size());

            OfferRecommendation rec = recommendations.get(0);
            assertEquals(Operation.Type.DESTROY, rec.getOperation().getType());

            rec = recommendations.get(1);
            assertEquals(Operation.Type.UNRESERVE, rec.getOperation().getType());
        }
    }

    @Test
    public void testUnexpectedResource() {
        List<Offer> offers = OfferTestUtils.getOffers(UNEXPECTED_RESOURCE_2);

        for (ResourceCleaner cleaner : allCleaners) {
            List<OfferRecommendation> recommendations = cleaner.evaluate(offers);

            assertEquals("Got: " + recommendations, 1, recommendations.size());

            assertEquals(Operation.Type.UNRESERVE, recommendations.get(0).getOperation().getType());
        }
    }

    @Test
    public void testUnexpectedMix() {
        List<Offer> offers = Arrays.asList(
                OfferTestUtils.getOffer(UNEXPECTED_RESOURCE_1),
                OfferTestUtils.getOffer(UNEXPECTED_RESOURCE_2),
                OfferTestUtils.getOffer(UNEXPECTED_RESOURCE_3));

        for (ResourceCleaner cleaner : allCleaners) {
            List<OfferRecommendation> recommendations = cleaner.evaluate(offers);

            assertEquals("Got: " + recommendations, 5, recommendations.size());

            // all destroy operations, followed by all unreserve operations

            OfferRecommendation rec = recommendations.get(0);
            assertEquals(Operation.Type.DESTROY, rec.getOperation().getType());
            assertEquals(UNEXPECTED_RESOURCE_1_ID,
                    getResourceId(rec));

            rec = recommendations.get(1);
            assertEquals(Operation.Type.DESTROY, rec.getOperation().getType());
            assertEquals(UNEXPECTED_RESOURCE_3_ID,
                    getResourceId(rec));

            rec = recommendations.get(2);
            assertEquals(Operation.Type.UNRESERVE, rec.getOperation().getType());
            assertEquals(UNEXPECTED_RESOURCE_1_ID,
                    getResourceId(rec));

            rec = recommendations.get(3);
            assertEquals(Operation.Type.UNRESERVE, rec.getOperation().getType());
            assertEquals(UNEXPECTED_RESOURCE_2_ID,
                    getResourceId(rec));

            rec = recommendations.get(4);
            assertEquals(Operation.Type.UNRESERVE, rec.getOperation().getType());
            assertEquals(UNEXPECTED_RESOURCE_3_ID,
                    getResourceId(rec));
        }
    }

    @Test
    public void testEmptyCleanersAllUnexpected() {
        List<Offer> offers = Arrays.asList(
                OfferTestUtils.getOffer(UNEXPECTED_RESOURCE_1),
                OfferTestUtils.getOffer(EXPECTED_RESOURCE_1),
                OfferTestUtils.getOffer(UNEXPECTED_RESOURCE_2),
                OfferTestUtils.getOffer(EXPECTED_RESOURCE_2),
                OfferTestUtils.getOffer(UNEXPECTED_RESOURCE_3));

        // This cleaner doesn't have any expected resources. everything above is "unexpected":
        List<OfferRecommendation> recommendations = emptyCleaner.evaluate(offers);

        assertEquals("Got: " + recommendations, 8, recommendations.size());

        // All destroy operations, followed by all unreserve operations

        OfferRecommendation rec = recommendations.get(0);
        assertEquals(Operation.Type.DESTROY, rec.getOperation().getType());
        assertEquals(UNEXPECTED_RESOURCE_1_ID, getResourceId(rec));

        rec = recommendations.get(1);
        assertEquals(Operation.Type.DESTROY, rec.getOperation().getType());
        assertEquals(EXPECTED_RESOURCE_2_ID, getResourceId(rec));

        rec = recommendations.get(2);
        assertEquals(Operation.Type.DESTROY, rec.getOperation().getType());
        assertEquals(UNEXPECTED_RESOURCE_3_ID, getResourceId(rec));

        rec = recommendations.get(3);
        assertEquals(Operation.Type.UNRESERVE, rec.getOperation().getType());
        assertEquals(UNEXPECTED_RESOURCE_1_ID, getResourceId(rec));

        rec = recommendations.get(4);
        assertEquals(Operation.Type.UNRESERVE, rec.getOperation().getType());
        assertEquals(EXPECTED_RESOURCE_1_ID, getResourceId(rec));

        rec = recommendations.get(5);
        assertEquals(Operation.Type.UNRESERVE, rec.getOperation().getType());
        assertEquals(UNEXPECTED_RESOURCE_2_ID, getResourceId(rec));

        rec = recommendations.get(6);
        assertEquals(Operation.Type.UNRESERVE, rec.getOperation().getType());
        assertEquals(EXPECTED_RESOURCE_2_ID, getResourceId(rec));

        rec = recommendations.get(7);
        assertEquals(Operation.Type.UNRESERVE, rec.getOperation().getType());
        assertEquals(UNEXPECTED_RESOURCE_3_ID, getResourceId(rec));
    }

    @Test
    public void testPopulatedCleanersSomeExpected() {
        List<Offer> offers = Arrays.asList(
                OfferTestUtils.getOffer(UNEXPECTED_RESOURCE_1),
                OfferTestUtils.getOffer(EXPECTED_RESOURCE_1),
                OfferTestUtils.getOffer(UNEXPECTED_RESOURCE_2),
                OfferTestUtils.getOffer(EXPECTED_RESOURCE_2),
                OfferTestUtils.getOffer(UNEXPECTED_RESOURCE_3));

        // this cleaner has expected resources populated, so they are omitted from the response:
        List<OfferRecommendation> recommendations = populatedCleaner.evaluate(offers);

        assertEquals("Got: " + recommendations, 5, recommendations.size());

        // all destroy operations, followed by all unreserve operations

        OfferRecommendation rec = recommendations.get(0);
        assertEquals(Operation.Type.DESTROY, rec.getOperation().getType());
        assertEquals(UNEXPECTED_RESOURCE_1_ID, getResourceId(rec));

        rec = recommendations.get(1);
        assertEquals(Operation.Type.DESTROY, rec.getOperation().getType());
        assertEquals(UNEXPECTED_RESOURCE_3_ID, getResourceId(rec));

        rec = recommendations.get(2);
        assertEquals(Operation.Type.UNRESERVE, rec.getOperation().getType());
        assertEquals(UNEXPECTED_RESOURCE_1_ID, getResourceId(rec));

        rec = recommendations.get(3);
        assertEquals(Operation.Type.UNRESERVE, rec.getOperation().getType());
        assertEquals(UNEXPECTED_RESOURCE_2_ID, getResourceId(rec));

        rec = recommendations.get(4);
        assertEquals(Operation.Type.UNRESERVE, rec.getOperation().getType());
        assertEquals(UNEXPECTED_RESOURCE_3_ID, getResourceId(rec));
    }

    @Test
    public void testPopulatedCleanersSomeExpectedDifferentOrder() {
        Offer offer = OfferTestUtils.getOffer(Arrays.asList(EXPECTED_RESOURCE_1, EXPECTED_RESOURCE_2,
                UNEXPECTED_RESOURCE_1, UNEXPECTED_RESOURCE_2));
        List<OfferRecommendation> recommendations = populatedCleaner.evaluate(Collections.singletonList(offer));
        // all destroy operations, followed by all unreserve operations

        assertEquals("Got: " + recommendations, 3, recommendations.size());

        OfferRecommendation rec = recommendations.get(0);
        assertEquals(Operation.Type.DESTROY, rec.getOperation().getType());
        assertEquals(UNEXPECTED_RESOURCE_1_ID, getResourceId(rec));

        rec = recommendations.get(1);
        assertEquals(Operation.Type.UNRESERVE, rec.getOperation().getType());
        assertEquals(UNEXPECTED_RESOURCE_2_ID, getResourceId(rec));

        rec = recommendations.get(2);
        assertEquals(Operation.Type.UNRESERVE, rec.getOperation().getType());
        assertEquals(UNEXPECTED_RESOURCE_1_ID, getResourceId(rec));
    }

    @Test
    public void testPopulatedCleanersAllExpected() {
        List<Offer> offers = OfferTestUtils.getOffers(Arrays.asList(EXPECTED_RESOURCE_1, EXPECTED_RESOURCE_2));

        // this cleaner has expected resources populated, so they are omitted from the response:
        List<OfferRecommendation> recommendations = populatedCleaner.evaluate(offers);
        assertEquals("Got: " + recommendations, 0, recommendations.size());
    }

    @Test
    public void testPopulatedCleanersNoneExpected() {
        Offer offer = OfferTestUtils.getOffer(Arrays.asList(UNEXPECTED_RESOURCE_1, UNEXPECTED_RESOURCE_2));
        List<OfferRecommendation> recommendations = populatedCleaner.evaluate(Collections.singletonList(offer));
        // all destroy operations, followed by all unreserve operations

        assertEquals("Got: " + recommendations, 3, recommendations.size());

        OfferRecommendation rec = recommendations.get(0);
        assertEquals(Operation.Type.DESTROY, rec.getOperation().getType());
        assertEquals(UNEXPECTED_RESOURCE_1_ID, getResourceId(rec));

        rec = recommendations.get(1);
        assertEquals(Operation.Type.UNRESERVE, rec.getOperation().getType());
        assertEquals(UNEXPECTED_RESOURCE_2_ID, getResourceId(rec));

        rec = recommendations.get(2);
        assertEquals(Operation.Type.UNRESERVE, rec.getOperation().getType());
        assertEquals(UNEXPECTED_RESOURCE_1_ID, getResourceId(rec));
    }

    @Test
    public void testPopulatedCleanersExpectedPartial() {
        Offer offer = OfferTestUtils.getOffer(Arrays.asList(EXPECTED_RESOURCE_1, UNEXPECTED_RESOURCE_2));

        List<OfferRecommendation> recommendations = populatedCleaner.evaluate(Collections.singletonList(offer));
        assertEquals("Got: " + recommendations, 1, recommendations.size());

        OfferRecommendation rec = recommendations.get(0);
        assertEquals(Operation.Type.UNRESERVE, rec.getOperation().getType());
    }

    private static String getResourceId(OfferRecommendation rec) {
        return ResourceTestUtils.getResourceId(((UninstallRecommendation) rec).getResource());
    }
}
