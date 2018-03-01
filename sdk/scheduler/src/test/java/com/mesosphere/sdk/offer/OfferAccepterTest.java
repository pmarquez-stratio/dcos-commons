package com.mesosphere.sdk.offer;

import com.mesosphere.sdk.offer.taskdata.TaskLabelWriter;
import com.mesosphere.sdk.scheduler.Driver;
import com.mesosphere.sdk.testutils.OfferTestUtils;
import com.mesosphere.sdk.testutils.TestConstants;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Offer.Operation;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.SchedulerDriver;
import com.mesosphere.sdk.testutils.ResourceTestUtils;
import com.mesosphere.sdk.testutils.TaskTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class OfferAccepterTest {

    @Mock
    private SchedulerDriver driver;

    @Before
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testLaunchTransient() {
        Resource resource = ResourceTestUtils.getUnreservedCpus(1.0);
        Offer offer = OfferTestUtils.getCompleteOffer(resource);
        TaskInfo.Builder taskInfoBuilder = TaskTestUtils.getTaskInfo(resource).toBuilder();
        taskInfoBuilder.setLabels(new TaskLabelWriter(taskInfoBuilder).setTransient().toProto());

        TestOperationRecorder recorder = new TestOperationRecorder();
        OfferAccepter accepter = new OfferAccepter("foo", Arrays.asList(recorder));
        Driver.setDriver(driver);
        accepter.accept(
                Arrays.asList(new LaunchOfferRecommendation(
                        offer,
                        taskInfoBuilder.build(),
                        Protos.ExecutorInfo.newBuilder().setExecutorId(TestConstants.EXECUTOR_ID).build(),
                        false,
                        true)));
        Assert.assertEquals(1, recorder.getLaunches().size());
        verify(driver, times(0)).acceptOffers(
                anyCollectionOf(OfferID.class),
                anyCollectionOf(Operation.class),
                anyObject());
    }

    @Test
    public void testLaunchTransientCustomExecutor() {
        Resource resource = ResourceTestUtils.getUnreservedCpus(1.0);
        Offer offer = OfferTestUtils.getOffer(resource);
        TaskInfo.Builder taskInfoBuilder = TaskTestUtils.getTaskInfo(resource).toBuilder();
        taskInfoBuilder.setLabels(new TaskLabelWriter(taskInfoBuilder).setTransient().toProto());

        TestOperationRecorder recorder = new TestOperationRecorder();
        OfferAccepter accepter = new OfferAccepter("foo", Arrays.asList(recorder));
        Driver.setDriver(driver);
        accepter.accept(
                Arrays.asList(new LaunchOfferRecommendation(
                        offer,
                        taskInfoBuilder.build(),
                        Protos.ExecutorInfo.newBuilder().setExecutorId(TestConstants.EXECUTOR_ID).build(),
                        false,
                        false)));
        Assert.assertEquals(1, recorder.getLaunches().size());
        verify(driver, times(0)).acceptOffers(
                anyCollectionOf(OfferID.class),
                anyCollectionOf(Operation.class),
                anyObject());
    }

    @Test
    public void testResourceOffers() {
        scheduler.resourceOffers(offers);
        verify(offerAccepter, times(2)).accept(any());
    }

    @Test
    public void testGroupRecommendationsByAgent() {
        Protos.Offer offerA = Protos.Offer.newBuilder(Protos.Offer.getDefaultInstance())
                .setHostname("A")
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("A").build())
                .setId(Protos.OfferID.newBuilder().setValue("A"))
                .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("A"))
                .build();
        Protos.Offer offerB = Protos.Offer.newBuilder(Protos.Offer.getDefaultInstance())
                .setHostname("B")
                .setSlaveId(Protos.SlaveID.newBuilder().setValue("B").build())
                .setId(Protos.OfferID.newBuilder().setValue("B"))
                .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("B"))
                .build();

        List<Protos.Offer> offers = Arrays.asList(offerA, offerB);

        final DestroyOfferRecommendation destroyRecommendationA =
                new DestroyOfferRecommendation(offerA, ResourceTestUtils.getUnreservedCpus(1.0));
        final DestroyOfferRecommendation destroyRecommendationB =
                new DestroyOfferRecommendation(offerB, ResourceTestUtils.getUnreservedCpus(1.0));
        final UnreserveOfferRecommendation unreserveRecommendationA =
                new UnreserveOfferRecommendation(offerA, ResourceTestUtils.getUnreservedCpus(1.0));
        final UnreserveOfferRecommendation unreserveRecommendationB =
                new UnreserveOfferRecommendation(offerB, ResourceTestUtils.getUnreservedCpus(1.0));

        List<OfferRecommendation> recommendations = Arrays.asList(
                destroyRecommendationA,
                destroyRecommendationB,
                unreserveRecommendationA,
                unreserveRecommendationB);
        when(resourceCleaner.evaluate(offers)).thenReturn(recommendations);

        final Map<Protos.SlaveID, List<OfferRecommendation>> group = scheduler.groupByAgent(recommendations);
        Assert.assertTrue(group.size() == 2);
        Protos.SlaveID prevSlaveID = null;
        for (Map.Entry<Protos.SlaveID, List<OfferRecommendation>> entry : group.entrySet()) {
            final List<OfferRecommendation> recommendations = entry.getValue();
            Assert.assertNotNull(recommendations);
            Assert.assertTrue(recommendations.size() == 2);
            final Protos.SlaveID key = entry.getKey();
            Assert.assertEquals(key, recommendations.get(0).getOffer().getSlaveId());
            Assert.assertEquals(key, recommendations.get(1).getOffer().getSlaveId());

            if (prevSlaveID != null) {
                Assert.assertNotEquals(key, prevSlaveID);
            }

            prevSlaveID = key;
        }
    }

    public static class TestOperationRecorder implements OperationRecorder {
        private List<Operation> reserves = new ArrayList<>();
        private List<Operation> unreserves = new ArrayList<>();
        private List<Operation> creates = new ArrayList<>();
        private List<Operation> destroys = new ArrayList<>();
        private List<Operation> launches = new ArrayList<>();

        public void record(Collection<OfferRecommendation> offerRecommendations) throws Exception {
            for (OfferRecommendation offerRecommendation : offerRecommendations) {
                Operation operation = offerRecommendation.getOperation();
                switch (operation.getType()) {
                    case UNRESERVE:
                        unreserves.add(operation);
                        break;
                    case RESERVE:
                        reserves.add(operation);
                        break;
                    case CREATE:
                        creates.add(operation);
                        break;
                    case DESTROY:
                        destroys.add(operation);
                        break;
                    case LAUNCH_GROUP:
                        launches.add(operation);
                        break;
                    case LAUNCH:
                        launches.add(operation);
                        break;
                    default:
                        throw new Exception("Unknown operation type encountered");
                }
            }
        }

        public List<Operation> getReserves() {
            return reserves;
        }

        public List<Operation> getUnreserves() {
            return unreserves;
        }

        public List<Operation> getCreates() {
            return creates;
        }

        public List<Operation> getDestroys() {
            return destroys;
        }

        public List<Operation> getLaunches() {
            return launches;
        }
    }
}
