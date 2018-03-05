package com.mesosphere.sdk.queues.scheduler;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.mesosphere.sdk.queues.http.types.JobInfoProvider;
import com.mesosphere.sdk.scheduler.ServiceScheduler;
import com.mesosphere.sdk.scheduler.plan.PlanCoordinator;
import com.mesosphere.sdk.specification.ServiceSpec;
import com.mesosphere.sdk.state.ConfigStore;
import com.mesosphere.sdk.state.StateStore;

/**
 * Default implementation of {@link JobInfoProvider} which also handles central storage of running jobs.
 *
 * The interface represents what's visible to job HTTP resources, whereas this implementation represents what's
 * available to {@code JobsEventClient}.
 */
public class DefaultJobInfoProvider implements JobInfoProvider {

    private final ReadWriteLock internalLock = new ReentrantReadWriteLock();
    private final Lock rlock = internalLock.readLock();
    private final Lock rwlock = internalLock.writeLock();

    private final Map<String, ServiceScheduler> jobs = new HashMap<>();

    /**
     * Returns the {@link StateStore} for the specified job, or an empty {@link Optional} if the job was not found.
     */
    @Override
    public Optional<StateStore> getStateStore(String jobName) {
        ServiceScheduler scheduler = getJob(jobName);
        return scheduler == null ? Optional.empty() : Optional.of(scheduler.getStateStore());
    }

    /**
     * Returns the {@link ConfigStore} for the specified job, or an empty {@link Optional} if the job was not found.
     */
    @Override
    public Optional<ConfigStore<ServiceSpec>> getConfigStore(String jobName) {
        ServiceScheduler scheduler = getJob(jobName);
        return scheduler == null ? Optional.empty() : Optional.of(scheduler.getConfigStore());
    }

    /**
     * Returns the {@link PlanCoordinator} for the specified job, or an empty {@link Optional} if the job was not found.
     */
    @Override
    public Optional<PlanCoordinator> getPlanCoordinator(String jobName) {
        ServiceScheduler scheduler = getJob(jobName);
        return scheduler == null ? Optional.empty() : Optional.of(scheduler.getPlanCoordinator());
    }

    /**
     * Returns the list of currently available jobs.
     */
    @Override
    public Collection<String> getJobs() {
        Collection<String> jobNames = new TreeSet<>(); // Alphabetical order
        lockAllR();
        try {
            jobNames.addAll(jobs.keySet());
        } finally {
            unlockR();
        }
        return jobNames;
    }

    /**
     * Returns the specified job, or null if it's not found.
     */
    private ServiceScheduler getJob(String jobName) {
        lockAllR();
        try {
            return jobs.get(jobName);
        } finally {
            unlockR();
        }
    }

    /**
     * Sets a shared read lock and returns the job values.
     */
    public Collection<ServiceScheduler> lockAllR() {
        rlock.lock();
        return jobs.values();
    }

    /**
     * Sets a shared read lock and returns the specified job, or {@code null} if it's not found. In either case,
     * {@link #unlockR()} must be called afterwards.
     */
    public ServiceScheduler lockJobR(String jobName) {
        rlock.lock();
        return jobs.get(jobName);
    }

    /**
     * Unlocks after a prior {@link #lockAllR()} or {@link #lockJobR(String)}.
     */
    public void unlockR() {
        rlock.unlock();
    }

    /**
     * Sets an exclusive write lock and returns the full job map.
     */
    public Map<String, ServiceScheduler> lockRW() {
        rwlock.lock();
        return jobs;
    }

    /**
     * Unlocks after a prior {@link #lockRW()}.
     */
    public void unlockRW() {
        rwlock.unlock();
    }
}
