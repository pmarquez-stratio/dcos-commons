package com.mesosphere.sdk.queues.state;

import com.mesosphere.sdk.queues.generator.Generator;
import com.mesosphere.sdk.scheduler.AbstractScheduler;
import com.mesosphere.sdk.specification.ServiceSpec;
import com.mesosphere.sdk.state.StateStore;
import com.mesosphere.sdk.storage.MemPersister;
import com.mesosphere.sdk.storage.Persister;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import static org.mockito.Mockito.*;

/**
 * This class tests the {@link SpecStore}.
 */
public class SpecStoreTest {
    private static final byte[] DATA1 = "this is a test".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DATA2 = "this is a different test".getBytes(StandardCharsets.UTF_8);

    @Mock private Generator mockGenerator1;
    @Mock private Generator mockGenerator2;
    @Mock private AbstractScheduler mockScheduler1;
    @Mock private AbstractScheduler mockScheduler2;
    @Mock private ServiceSpec mockServiceSpec1;
    @Mock private ServiceSpec mockServiceSpec2;

    private Persister persister;
    private SpecStore specStore;
    private StateStore stateStore1;
    private StateStore stateStore2;

    @Before
    public void beforeEach() {
        MockitoAnnotations.initMocks(this);
        when(mockScheduler1.getServiceSpec()).thenReturn(mockServiceSpec1);
        when(mockScheduler2.getServiceSpec()).thenReturn(mockServiceSpec2);
        when(mockServiceSpec1.getName()).thenReturn("scheduler1");
        when(mockServiceSpec2.getName()).thenReturn("scheduler2");
        persister = new MemPersister();
        specStore = new SpecStore(persister);
        stateStore1 = new StateStore(persister, "1");
        stateStore2 = new StateStore(persister, "2");
    }

    @Test
    public void storeDifferentDataSameType() throws Exception {
        specStore.store(stateStore1, DATA1, "type");
        specStore.store(stateStore2, DATA2, "type");
        String id1str = "type-" + DigestUtils.sha256Hex(DATA1);
        String id2str = "type-" + DigestUtils.sha256Hex(DATA2);

        Assert.assertEquals(id1str, new String(stateStore1.fetchProperty("spec-id"), StandardCharsets.UTF_8));
        Assert.assertEquals(id2str, new String(stateStore2.fetchProperty("spec-id"), StandardCharsets.UTF_8));
        Collection<String> storedSpecIds = persister.getChildren("Specs");
        Assert.assertEquals(2, storedSpecIds.size());
        Assert.assertTrue(storedSpecIds.containsAll(Arrays.asList(id1str, id2str)));

        when(mockGenerator1.generate(any())).thenReturn(mockScheduler1);
        Collection<AbstractScheduler> schedulers = specStore.recover(Collections.singletonMap("type", mockGenerator1));
        Assert.assertEquals(2, schedulers.size());
        for (AbstractScheduler s : schedulers) {
            Assert.assertEquals(mockScheduler1, s);
        }
        verify(mockGenerator1).generate(DATA1);
        verify(mockGenerator1).generate(DATA2);
    }

    @Test
    public void storeDifferentDataDifferentType() throws Exception {
        specStore.store(stateStore1, DATA1, "type1");
        specStore.store(stateStore2, DATA2, "type2");
        String id1str = "type1-" + DigestUtils.sha256Hex(DATA1);
        String id2str = "type2-" + DigestUtils.sha256Hex(DATA2);

        Assert.assertEquals(id1str, new String(stateStore1.fetchProperty("spec-id"), StandardCharsets.UTF_8));
        Assert.assertEquals(id2str, new String(stateStore2.fetchProperty("spec-id"), StandardCharsets.UTF_8));
        Collection<String> storedSpecIds = persister.getChildren("Specs");
        Assert.assertEquals(2, storedSpecIds.size());
        Assert.assertTrue(storedSpecIds.containsAll(Arrays.asList(id1str, id2str)));

        when(mockGenerator1.generate(any())).thenReturn(mockScheduler1);
        when(mockGenerator2.generate(any())).thenReturn(mockScheduler2);
        Map<String, Generator> generators = new HashMap<>();
        generators.put("type1", mockGenerator1);
        generators.put("type2", mockGenerator2);
        Collection<AbstractScheduler> schedulers = specStore.recover(generators);
        Assert.assertEquals(2, schedulers.size());
        Assert.assertTrue(schedulers.contains(mockScheduler1));
        Assert.assertTrue(schedulers.contains(mockScheduler2));
        verify(mockGenerator1).generate(DATA1);
        verify(mockGenerator2).generate(DATA2);
    }

    @Test
    public void storeSameDataDifferentType() throws Exception {
        specStore.store(stateStore1, DATA1, "type1");
        specStore.store(stateStore2, DATA1, "type2");
        String id1str = "type1-" + DigestUtils.sha256Hex(DATA1);
        String id2str = "type2-" + DigestUtils.sha256Hex(DATA1);

        Assert.assertEquals(id1str, new String(stateStore1.fetchProperty("spec-id"), StandardCharsets.UTF_8));
        Assert.assertEquals(id2str, new String(stateStore2.fetchProperty("spec-id"), StandardCharsets.UTF_8));
        Collection<String> storedSpecIds = persister.getChildren("Specs");
        Assert.assertEquals(2, storedSpecIds.size());
        Assert.assertTrue(storedSpecIds.containsAll(Arrays.asList(id1str, id2str)));

        when(mockGenerator1.generate(any())).thenReturn(mockScheduler1);
        when(mockGenerator2.generate(any())).thenReturn(mockScheduler2);
        Map<String, Generator> generators = new HashMap<>();
        generators.put("type1", mockGenerator1);
        generators.put("type2", mockGenerator2);
        Collection<AbstractScheduler> schedulers = specStore.recover(generators);
        Assert.assertEquals(2, schedulers.size());
        Assert.assertTrue(schedulers.contains(mockScheduler1));
        Assert.assertTrue(schedulers.contains(mockScheduler2));
        verify(mockGenerator1).generate(DATA1);
        verify(mockGenerator2).generate(DATA1);
    }

    @Test
    public void storeSameDataSameType() throws Exception {
        specStore.store(stateStore1, DATA1, "type");
        specStore.store(stateStore2, DATA1, "type");
        String idstr = "type-" + DigestUtils.sha256Hex(DATA1);

        Assert.assertEquals(idstr, new String(stateStore1.fetchProperty("spec-id"), StandardCharsets.UTF_8));
        Assert.assertEquals(idstr, new String(stateStore2.fetchProperty("spec-id"), StandardCharsets.UTF_8));
        Collection<String> storedSpecIds = persister.getChildren("Specs");
        Assert.assertEquals(Collections.singleton(idstr), storedSpecIds);

        when(mockGenerator1.generate(any())).thenReturn(mockScheduler1);
        Collection<AbstractScheduler> schedulers = specStore.recover(Collections.singletonMap("type", mockGenerator1));
        Assert.assertEquals(2, schedulers.size());
        for (AbstractScheduler s : schedulers) {
            Assert.assertEquals(mockScheduler1, s);
        }
        verify(mockGenerator1, times(2)).generate(DATA1);
    }
}
