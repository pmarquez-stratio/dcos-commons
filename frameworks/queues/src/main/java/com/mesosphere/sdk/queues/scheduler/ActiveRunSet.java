package com.mesosphere.sdk.queues.scheduler;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.mesosphere.sdk.scheduler.AbstractScheduler;

/**
 * In-memory storage of active runs. Access is shared by all components that need to access active runs.
 */
public class ActiveRunSet {

    private final ReadWriteLock internalLock = new ReentrantReadWriteLock();
    private final Lock rlock = internalLock.readLock();
    private final Lock rwlock = internalLock.writeLock();

    private final Map<String, AbstractScheduler> runs = new HashMap<>();

    /**
     * Returns the names of all runs, or an empty collection if none are found.
     */
    public Collection<String> getRunNames() {
        Collection<String> runNames = new TreeSet<>(); // Alphabetical order
        lockR();
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
        lockR();
        try {
            return runs.get(runName);
        } finally {
            unlockR();
        }
    }

    /**
     * Sets a shared read lock and returns the run values. {@link #unlockR()} must be called afterwards.
     */
    public Collection<AbstractScheduler> lockR() {
        rlock.lock();
        return runs.values();
    }

    /**
     * Unlocks after a prior {@link #lockR()}.
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
