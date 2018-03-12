package com.mesosphere.sdk.queues.http.types;

import java.util.Collection;
import java.util.Optional;

import com.mesosphere.sdk.scheduler.AbstractScheduler;

/**
 * Interface for listing/adding/removing active runs via HTTP management endpoints.
 */
public interface RunManager {

    /**
     * Returns the list of currently available runs.
     */
    public Collection<String> getRunNames();

    /**
     * Adds a run which is mapped for the specified name. Note: If the run was marked for uninstall via
     * {@link #uninstallRun(String)}, it should continue to be added across scheduler restarts in order for uninstall to
     * complete. It should only be omitted after the uninstall callback has been invoked for it.
     *
     * @param scheduler the run to add
     * @return {@code this}
     * @throws IllegalArgumentException if the run name is already present
     */
    public RunManager putRun(AbstractScheduler scheduler);

    /**
     * Returns the specified run, or an empty {@code Optional} if it's not found.
     */
    public Optional<AbstractScheduler> getRun(String runName);

    /**
     * Triggers uninstall of the specified run. After uninstall is complete, it should be removed using
     * {@link #removeRuns(Collection)}.
     *
     * @throws IllegalArgumentException if the run wasn't found
     */
    public void uninstallRun(String runName) throws IllegalArgumentException;
}
