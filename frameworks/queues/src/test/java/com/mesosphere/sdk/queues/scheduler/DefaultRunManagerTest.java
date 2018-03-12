package com.mesosphere.sdk.queues.scheduler;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.mesosphere.sdk.scheduler.DefaultScheduler;
import com.mesosphere.sdk.scheduler.uninstall.UninstallScheduler;
import com.mesosphere.sdk.specification.ServiceSpec;

public class DefaultRunManagerTest {

    @Mock private ServiceSpec mockServiceSpec1;
    @Mock private ServiceSpec mockServiceSpec2;
    @Mock private DefaultScheduler mockClient1;
    @Mock private DefaultScheduler mockClient2;
    @Mock private ServiceSpec mockUninstallServiceSpec;
    @Mock private UninstallScheduler mockUninstallClient;

    private DefaultRunManager runManager;

    @Before
    public void beforeEach() {
        MockitoAnnotations.initMocks(this);
        when(mockClient1.getServiceSpec()).thenReturn(mockServiceSpec1);
        when(mockClient2.getServiceSpec()).thenReturn(mockServiceSpec2);
        when(mockServiceSpec1.getName()).thenReturn("1");
        when(mockServiceSpec2.getName()).thenReturn("2");
        runManager = new DefaultRunManager(new ActiveRunSet());
    }

    @Test
    public void putClientsDuplicate() {
        runManager.putRun(mockClient1);
        runManager.putRun(mockClient2);
        try {
            runManager.putRun(mockClient2);
            Assert.fail("Expected exception: duplicate key");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void putClientsRegistration() {
        runManager.putRun(mockClient1);
        runManager.registered(false);
        verify(mockClient1).registered(false);
        runManager.putRun(mockClient2);
        // Should have been called automatically due to already being registered:
        verify(mockClient2).registered(false);
        try {
            runManager.putRun(mockClient2);
            Assert.fail("Expected exception: duplicate key");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void uninstallRequestedClient() {
        when(mockClient1.toUninstallScheduler()).thenReturn(mockUninstallClient);
        when(mockUninstallClient.getServiceSpec()).thenReturn(mockUninstallServiceSpec);
        when(mockUninstallServiceSpec.getName()).thenReturn("1");
        runManager.putRun(mockClient1);
        // 2 and second 1 are ignored:
        runManager.uninstallRuns(Arrays.asList("1", "1", "2"));
        Assert.assertEquals(1, runManager.getRunNames().size());
        verify(mockClient1).toUninstallScheduler();

        runManager.removeRuns(Collections.singleton("1"));
        Assert.assertEquals(0, runManager.getRunNames().size());
    }
}
