package com.mesosphere.sdk.queues.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.mesosphere.sdk.scheduler.DefaultScheduler;
import com.mesosphere.sdk.offer.CommonIdUtils;
import com.mesosphere.sdk.offer.Constants;
import com.mesosphere.sdk.offer.OfferRecommendation;
import com.mesosphere.sdk.offer.ReserveOfferRecommendation;
import com.mesosphere.sdk.scheduler.MesosEventClient.OfferResponse;
import com.mesosphere.sdk.scheduler.MesosEventClient.StatusResponse;
import com.mesosphere.sdk.scheduler.SchedulerConfig;
import com.mesosphere.sdk.scheduler.uninstall.UninstallScheduler;

import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class QueueEventClientTest {

    private static final Answer<OfferResponse> CONSUME_FIRST_OFFER = new Answer<OfferResponse>() {
        @Override
        public OfferResponse answer(InvocationOnMock invocation) throws Throwable {
            List<Offer> offers = getOffersArgument(invocation);
            if (offers.isEmpty()) {
                return OfferResponse.processed(Collections.emptyList());
            }
            return OfferResponse.processed(Collections.singletonList(
                    new ReserveOfferRecommendation(offers.get(0), getUnreservedCpus(3))));
        }
    };

    private static final Answer<OfferResponse> CONSUME_LAST_OFFER = new Answer<OfferResponse>() {
        @Override
        public OfferResponse answer(InvocationOnMock invocation) throws Throwable {
            List<Offer> offers = getOffersArgument(invocation);
            if (offers.isEmpty()) {
                return OfferResponse.processed(Collections.emptyList());
            }
            return OfferResponse.processed(Collections.singletonList(
                    new ReserveOfferRecommendation(offers.get(offers.size() - 1), getUnreservedCpus(5))));
        }
    };

    private static final Answer<OfferResponse> NO_CHANGES = new Answer<OfferResponse>() {
        @Override
        public OfferResponse answer(InvocationOnMock invocation) throws Throwable {
            return OfferResponse.processed(Collections.emptyList());
        }
    };

    private static final Answer<OfferResponse> OFFER_NOT_READY = new Answer<OfferResponse>() {
        @Override
        public OfferResponse answer(InvocationOnMock invocation) throws Throwable {
            return OfferResponse.notReady(Collections.emptyList());
        }
    };

    @Mock private DefaultScheduler mockClient1;
    @Mock private DefaultScheduler mockClient2;
    @Mock private DefaultScheduler mockClient3;
    @Mock private DefaultScheduler mockClient4;
    @Mock private DefaultScheduler mockClient5;
    @Mock private DefaultScheduler mockClient6;
    @Mock private DefaultScheduler mockClient7;
    @Mock private DefaultScheduler mockClient8;
    @Mock private DefaultScheduler mockClient9;
    @Mock private UninstallScheduler mockUninstallClient;
    @Mock private SchedulerConfig mockSchedulerConfig;
    @Mock private QueueEventClient.UninstallCallback mockUninstallCallback;

    private QueueEventClient client;

    @Before
    public void beforeEach() {
        MockitoAnnotations.initMocks(this);
        when(mockClient1.getName()).thenReturn("1");
        when(mockClient2.getName()).thenReturn("2");
        when(mockClient3.getName()).thenReturn("3");
        when(mockClient4.getName()).thenReturn("4");
        when(mockClient5.getName()).thenReturn("5");
        when(mockClient6.getName()).thenReturn("6");
        when(mockClient7.getName()).thenReturn("7");
        when(mockClient8.getName()).thenReturn("8");
        when(mockClient9.getName()).thenReturn("9");
        client = new QueueEventClient(mockSchedulerConfig, mockUninstallCallback);
    }

    @Test
    public void putClientsDuplicate() {
        client.putRun(mockClient1);
        client.putRun(mockClient2);
        try {
            client.putRun(mockClient2);
            Assert.fail("Expected exception: duplicate key");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void putClientsRegistration() {
        client.putRun(mockClient1);
        client.registered(false);
        verify(mockClient1).registered(false);
        client.putRun(mockClient2);
        // Should have been called automatically due to already being registered:
        verify(mockClient2).registered(false);
        try {
            client.putRun(mockClient2);
            Assert.fail("Expected exception: duplicate key");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void offerNoClients() {
        // No offers
        OfferResponse response = client.offers(Collections.emptyList());
        Assert.assertEquals(OfferResponse.Result.NOT_READY, response.result);
        Assert.assertTrue(response.recommendations.isEmpty());

        // Some offers
        response = client.offers(Arrays.asList(getOffer(1), getOffer(2), getOffer(3)));
        Assert.assertEquals(OfferResponse.Result.NOT_READY, response.result);
        Assert.assertTrue(response.recommendations.isEmpty());
    }

    @Test
    public void uninstallRequestedClient() {
        when(mockClient1.toUninstallScheduler()).thenReturn(mockUninstallClient);
        when(mockUninstallClient.offers(any())).thenReturn(OfferResponse.uninstalled());
        when(mockUninstallClient.getName()).thenReturn("1");
        client.putRun(mockClient1);
        client.uninstallRun("1");
        client.uninstallRun("1"); // no-op

        verifyZeroInteractions(mockUninstallCallback);
        client.offers(Collections.emptyList());
        verify(mockUninstallCallback).uninstalled("1");

        try {
            // Should fail since the client was removed from the map:
            client.uninstallRun("1");
            Assert.fail("Expected exception");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void uninstallFinishedClient() {
        when(mockClient1.toUninstallScheduler()).thenReturn(mockUninstallClient);
        when(mockClient1.offers(any())).thenReturn(OfferResponse.finished());
        client.putRun(mockClient1);

        // mockClient1 is converted to mockUninstallClient:
        client.offers(Collections.emptyList());
        verify(mockClient1).toUninstallScheduler();

        // mockUninstallClient is removed:
        when(mockUninstallClient.offers(any())).thenReturn(OfferResponse.uninstalled());
        when(mockUninstallClient.getName()).thenReturn("hello");

        verifyZeroInteractions(mockUninstallCallback);
        client.offers(Collections.emptyList());
        verify(mockUninstallCallback).uninstalled("hello");
    }

    @Test
    public void finishedAndUninstalled() {
        when(mockClient1.offers(any())).thenReturn(OfferResponse.uninstalled());
        when(mockClient2.offers(any())).thenReturn(OfferResponse.finished());
        when(mockClient3.offers(any())).thenReturn(OfferResponse.uninstalled());
        when(mockClient4.offers(any())).thenReturn(OfferResponse.finished());
        client
                .putRun(mockClient1)
                .putRun(mockClient2)
                .putRun(mockClient3)
                .putRun(mockClient4);
    }

    @Test
    public void offerPruning() {
        // Client 1,4,7: consumes the first offer
        // Client 2,5,8: consumes the last offer
        // Client 3,6,9: no change to offers
        when(mockClient1.offers(any())).then(CONSUME_FIRST_OFFER);
        when(mockClient2.offers(any())).then(CONSUME_LAST_OFFER);
        when(mockClient3.offers(any())).then(NO_CHANGES);
        when(mockClient4.offers(any())).then(CONSUME_FIRST_OFFER);
        when(mockClient5.offers(any())).then(CONSUME_LAST_OFFER);
        when(mockClient6.offers(any())).then(NO_CHANGES);
        when(mockClient7.offers(any())).then(CONSUME_FIRST_OFFER);
        when(mockClient8.offers(any())).then(CONSUME_LAST_OFFER);
        when(mockClient9.offers(any())).then(NO_CHANGES);
        client
                .putRun(mockClient1)
                .putRun(mockClient2)
                .putRun(mockClient3)
                .putRun(mockClient4)
                .putRun(mockClient5)
                .putRun(mockClient6)
                .putRun(mockClient7)
                .putRun(mockClient8)
                .putRun(mockClient9);

        // Empty offers: All clients should have been pinged regardless
        OfferResponse response = client.offers(Collections.emptyList());
        Assert.assertEquals(OfferResponse.Result.PROCESSED, response.result);
        Assert.assertTrue(response.recommendations.isEmpty());
        verify(mockClient1).offers(Collections.emptyList());
        verify(mockClient2).offers(Collections.emptyList());
        verify(mockClient3).offers(Collections.emptyList());
        verify(mockClient4).offers(Collections.emptyList());
        verify(mockClient5).offers(Collections.emptyList());
        verify(mockClient6).offers(Collections.emptyList());
        verify(mockClient7).offers(Collections.emptyList());
        verify(mockClient8).offers(Collections.emptyList());
        verify(mockClient9).offers(Collections.emptyList());

        // Seven offers: Only the middle offer is left at the end.
        Protos.Offer middleOffer = getOffer(4);
        Collection<Protos.Offer> offers = Arrays.asList(
                getOffer(1), getOffer(2), getOffer(3),
                middleOffer,
                getOffer(5), getOffer(6), getOffer(7));
        response = client.offers(offers);
        Assert.assertEquals(OfferResponse.Result.PROCESSED, response.result);
        Set<Integer> expectedConsumedOffers = new HashSet<>(Arrays.asList(1, 2, 3, 5, 6, 7));
        Assert.assertEquals(expectedConsumedOffers.size(), response.recommendations.size());
        for (OfferRecommendation rec : response.recommendations) {
            Assert.assertTrue(rec.getOffer().getId().getValue(),
                    expectedConsumedOffers.contains(Integer.parseInt(rec.getOffer().getId().getValue())));
        }
        // Verify that offers are consumed in the order we would expect:
        verify(mockClient1).offers(Arrays.asList(
                getOffer(1), getOffer(2), getOffer(3), middleOffer, getOffer(5), getOffer(6), getOffer(7)));
        verify(mockClient2).offers(Arrays.asList(
                getOffer(2), getOffer(3), middleOffer, getOffer(5), getOffer(6), getOffer(7))); // 1 ate first
        verify(mockClient3).offers(Arrays.asList(
                getOffer(2), getOffer(3), middleOffer, getOffer(5), getOffer(6))); // 2 ate last
        verify(mockClient4).offers(Arrays.asList(
                getOffer(2), getOffer(3), middleOffer, getOffer(5), getOffer(6))); // no change by 3
        verify(mockClient5).offers(Arrays.asList(
                getOffer(3), middleOffer, getOffer(5), getOffer(6))); // 4 ate first
        verify(mockClient6).offers(Arrays.asList(
                getOffer(3), middleOffer, getOffer(5))); // 5 ate last
        verify(mockClient7).offers(Arrays.asList(
                getOffer(3), middleOffer, getOffer(5))); // no change by 6
        verify(mockClient8).offers(Arrays.asList(
                middleOffer, getOffer(5))); // 7 ate first
        verify(mockClient9).offers(Arrays.asList(
                middleOffer)); // 8 ate last
    }

    @Test
    public void offerSomeClientsNotReady() {
        // One client: Not ready
        when(mockClient1.offers(any())).then(NO_CHANGES);
        when(mockClient2.offers(any())).then(OFFER_NOT_READY);
        when(mockClient3.offers(any())).then(NO_CHANGES);
        client
                .putRun(mockClient1)
                .putRun(mockClient2)
                .putRun(mockClient3);

        // Empty offers: All clients should have been pinged regardless
        OfferResponse response = client.offers(Collections.emptyList());
        Assert.assertEquals(OfferResponse.Result.NOT_READY, response.result);
        Assert.assertTrue(response.recommendations.isEmpty());
        verify(mockClient1).offers(Collections.emptyList());
        verify(mockClient2).offers(Collections.emptyList());
        verify(mockClient3).offers(Collections.emptyList());

        // Three offers: All clients should have been pinged with the same offers.
        List<Protos.Offer> offers = Arrays.asList(getOffer(1), getOffer(2), getOffer(3));
        response = client.offers(offers);
        Assert.assertEquals(OfferResponse.Result.NOT_READY, response.result);
        Assert.assertTrue(response.recommendations.isEmpty());
        verify(mockClient1).offers(offers);
        verify(mockClient2).offers(offers);
        verify(mockClient3).offers(offers);
    }

    @Test
    public void statusUnknown() {
        // Client 2: unknown task, Clients 1 and 3: no interaction
        when(mockClient2.status(any())).thenReturn(StatusResponse.unknownTask());
        client
                .putRun(mockClient1)
                .putRun(mockClient2)
                .putRun(mockClient3);

        Protos.TaskStatus status = getStatus("2");
        Assert.assertEquals(StatusResponse.Result.UNKNOWN_TASK, client.status(status).result);
        verify(mockClient1, never()).status(any());
        verify(mockClient2, times(1)).status(status);
        verify(mockClient3, never()).status(any());
    }

    @Test
    public void statusProcessed() {
        when(mockClient1.status(any())).thenReturn(StatusResponse.unknownTask());
        when(mockClient2.status(any())).thenReturn(StatusResponse.unknownTask());
        when(mockClient3.status(any())).thenReturn(StatusResponse.processed());
        when(mockClient4.status(any())).thenReturn(StatusResponse.unknownTask());
        client
                .putRun(mockClient1)
                .putRun(mockClient2)
                .putRun(mockClient3)
                .putRun(mockClient4);

        Protos.TaskStatus status = getStatus("3");
        Assert.assertEquals(StatusResponse.Result.PROCESSED, client.status(status).result);
        verify(mockClient1, never()).status(any());
        verify(mockClient2, never()).status(any());
        verify(mockClient3, times(1)).status(status);
        verify(mockClient4, never()).status(any());
    }

    @SuppressWarnings("deprecation")
    private static Protos.Resource getUnreservedCpus(double cpus) {
        Protos.Resource.Builder resBuilder = Protos.Resource.newBuilder()
                .setName("cpus")
                .setType(Protos.Value.Type.SCALAR)
                .setRole(Constants.ANY_ROLE);
        resBuilder.getScalarBuilder().setValue(cpus);
        return resBuilder.build();
    }

    private static Protos.TaskStatus getStatus(String clientName) {
        return Protos.TaskStatus.newBuilder()
                .setTaskId(CommonIdUtils.toTaskId(clientName, "foo"))
                .setState(TaskState.TASK_FINISHED)
                .build();
    }

    private static Protos.Offer getOffer(int id) {
        return Protos.Offer.newBuilder()
                .setId(Protos.OfferID.newBuilder().setValue(Integer.toString(id)))
                .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("test-framework-id").build())
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("test-slave-id").build())
                .setHostname("test-hostname")
                .build();
    }

    @SuppressWarnings("unchecked")
    private static List<Protos.Offer> getOffersArgument(InvocationOnMock invocation) {
        return (List<Protos.Offer>) invocation.getArguments()[0];
    }
}
