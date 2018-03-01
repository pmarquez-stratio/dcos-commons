package com.mesosphere.sdk.scheduler.uninstall;

import com.mesosphere.sdk.offer.CreateOfferRecommendation;
import com.mesosphere.sdk.offer.OfferRecommendation;
import com.mesosphere.sdk.offer.OperationRecorder;
import com.mesosphere.sdk.state.StateStore;
import com.mesosphere.sdk.testutils.ResourceTestUtils;

import org.apache.mesos.Protos;
import org.junit.Test;

import static org.mockito.Mockito.mock;

import java.util.Collections;

public class UninstallRecorderTest {

    @Test
    public void testHandlingOfUnexpectedOfferRecommendation() throws Exception {
        Protos.Resource resource = ResourceTestUtils.getUnreservedCpus(1.0);
        OfferRecommendation unsupportedOfferRecommendation = new CreateOfferRecommendation(null, resource);
        StateStore mockStateStore = mock(StateStore.class);
        OperationRecorder operationRecorder = new UninstallRecorder(mockStateStore, Collections.emptyList());
        // should just return without error
        operationRecorder.record(Collections.singletonList(unsupportedOfferRecommendation));
    }
}
