package com.mesosphere.sdk.queues.http.types;

import java.util.Collection;
import java.util.Optional;

import com.mesosphere.sdk.scheduler.plan.PlanCoordinator;
import com.mesosphere.sdk.specification.ServiceSpec;
import com.mesosphere.sdk.state.ConfigStore;
import com.mesosphere.sdk.state.StateStore;

/**
 * Retrieves objects which are necessary for servicing HTTP requests against per-run endpoints.
 */
public interface QueueInfoProvider {

    /**
     * Returns the list of currently available runs.
     */
    public Collection<String> getRuns();

    /**
     * Returns the {@link StateStore} for the specified run, or an empty {@link Optional} if the run was not found.
     */
    public Optional<StateStore> getStateStore(String runName);

    /**
     * Returns the {@link ConfigStore} for the specified run, or an empty {@link Optional} if the run was not found.
     */
    public Optional<ConfigStore<ServiceSpec>> getConfigStore(String runName);

    /**
     * Returns the {@link PlanCoordinator} for the specified run, or an empty {@link Optional} if the run was not found.
     */
    public Optional<PlanCoordinator> getPlanCoordinator(String runName);
}
