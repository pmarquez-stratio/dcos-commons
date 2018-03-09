package com.mesosphere.sdk.queues.scheduler;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.mesosphere.sdk.queues.http.types.QueueInfoProvider;
import com.mesosphere.sdk.scheduler.AbstractScheduler;
import com.mesosphere.sdk.scheduler.plan.PlanCoordinator;
import com.mesosphere.sdk.specification.ServiceSpec;
import com.mesosphere.sdk.state.ConfigStore;
import com.mesosphere.sdk.state.StateStore;

/**
 * Default implementation of {@link QueueInfoProvider} which also handles central storage of active runs.
 *
 * The interface represents what's visible to run HTTP resources, whereas this implementation represents what's
 * available to {@code runsEventClient}.
 */
public class DefaultQueueInfoProvider implements QueueInfoProvider {

    private final ReadWriteLock internalLock = new ReentrantReadWriteLock();
    private final Lock rlock = internalLock.readLock();
    private final Lock rwlock = internalLock.writeLock();

    private final Map<String, AbstractScheduler> runs = new HashMap<>();

    /**
     * Returns the {@link StateStore} for the specified run, or an empty {@link Optional} if the run was not found.
     */
    @Override
    public Optional<StateStore> getStateStore(String runName) {
        AbstractScheduler scheduler = getRun(runName);
        return scheduler == null ? Optional.empty() : Optional.of(scheduler.getStateStore());
    }

    /**
     * Returns the {@link ConfigStore} for the specified run, or an empty {@link Optional} if the run was not found.
     */
    @Override
    public Optional<ConfigStore<ServiceSpec>> getConfigStore(String runName) {
        AbstractScheduler scheduler = getRun(runName);
        return scheduler == null ? Optional.empty() : Optional.of(scheduler.getConfigStore());
    }

    /**
     * Returns the {@link PlanCoordinator} for the specified run, or an empty {@link Optional} if the run was not found.
     */
    @Override
    public Optional<PlanCoordinator> getPlanCoordinator(String runName) {
        AbstractScheduler scheduler = getRun(runName);
        return scheduler == null ? Optional.empty() : Optional.of(scheduler.getPlanCoordinator());
    }

    /**
     * Returns the list of currently available runs.
     */
    @Override
    public Collection<String> getRuns() {
        Collection<String> runNames = new TreeSet<>(); // Alphabetical order
        lockAllR();
        try {
            runNames.addAll(runs.keySet());
        } finally {
            unlockR();
        }
        return runNames;
    }

    /**
     * Returns the specified run, or {@code null} if it's not found.
     */
    public AbstractScheduler getRun(String runName) {
        lockAllR();
        try {
            return runs.get(runName);
        } finally {
            unlockR();
        }
    }

    /**
     * Sets a shared read lock and returns the run values. {@link #unlockR()} must be called afterwards.
     */
    public Collection<AbstractScheduler> lockAllR() {
        rlock.lock();
        return runs.values();
    }

    /**
     * Unlocks after a prior {@link #lockAllR()}.
     */
    public void unlockR() {
        rlock.unlock();
    }

    /**
     * Sets an exclusive write lock and returns the full run map. {@link #unlockRW()} must be called afterwards.
     */
    public Map<String, AbstractScheduler> lockRW() {
        rwlock.lock();
        return runs;
    }

    /**
     * Unlocks after a prior {@link #lockRW()}.
     */
    public void unlockRW() {
        rwlock.unlock();
    }
}
