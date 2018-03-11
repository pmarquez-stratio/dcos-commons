package com.mesosphere.sdk.queues.http.types;

import java.util.Optional;

import com.mesosphere.sdk.queues.scheduler.ActiveRunSet;
import com.mesosphere.sdk.scheduler.AbstractScheduler;
import com.mesosphere.sdk.scheduler.plan.PlanCoordinator;
import com.mesosphere.sdk.specification.ServiceSpec;
import com.mesosphere.sdk.state.ConfigStore;
import com.mesosphere.sdk.state.StateStore;

/**
 * Retrieves objects which are necessary for servicing HTTP queries describing individual runs.
 */
public class RunInfoProvider {

    private final ActiveRunSet store;

    public RunInfoProvider(ActiveRunSet store) {
        this.store = store;
    }

    /**
     * Returns the {@link StateStore} for the specified run, or an empty {@link Optional} if the run was not found.
     */
    public Optional<StateStore> getStateStore(String runName) {
        AbstractScheduler scheduler = store.getRun(runName);
        return scheduler == null ? Optional.empty() : Optional.of(scheduler.getStateStore());
    }

    /**
     * Returns the {@link ConfigStore} for the specified run, or an empty {@link Optional} if the run was not found.
     */
    public Optional<ConfigStore<ServiceSpec>> getConfigStore(String runName) {
        AbstractScheduler scheduler = store.getRun(runName);
        return scheduler == null ? Optional.empty() : Optional.of(scheduler.getConfigStore());
    }

    /**
     * Returns the {@link PlanCoordinator} for the specified run, or an empty {@link Optional} if the run was not found.
     */
    public Optional<PlanCoordinator> getPlanCoordinator(String runName) {
        AbstractScheduler scheduler = store.getRun(runName);
        return scheduler == null ? Optional.empty() : Optional.of(scheduler.getPlanCoordinator());
    }
}
