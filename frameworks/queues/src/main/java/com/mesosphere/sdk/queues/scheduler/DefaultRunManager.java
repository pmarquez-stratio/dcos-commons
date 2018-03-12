package com.mesosphere.sdk.queues.scheduler;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;

import com.mesosphere.sdk.offer.LoggingUtils;
import com.mesosphere.sdk.queues.http.types.RunInfoProvider;
import com.mesosphere.sdk.queues.http.types.RunManager;
import com.mesosphere.sdk.scheduler.AbstractScheduler;
import com.mesosphere.sdk.scheduler.DefaultScheduler;
import com.mesosphere.sdk.scheduler.uninstall.UninstallScheduler;

/**
 * Higher-level frontend to {@link ActiveRunSet} which handles adding/removing active runs.
 *
 * This implements both the {@link RunManager} interface for access by HTTP endpoints, plus any additional access
 * required by {@link QueueEventClient}.
 */
public class DefaultRunManager implements RunManager {

    private static final Logger LOGGER = LoggingUtils.getLogger(DefaultRunManager.class);

    private final ActiveRunSet activeRunSet;

    // Keeps track of whether we've had the registered callback yet.
    // When a client is added, if we're already registered then invoke 'registered()' manually against that client
    private boolean isRegistered;

    public DefaultRunManager(ActiveRunSet activeRunSet) {
        this.activeRunSet = activeRunSet;
        this.isRegistered = false;
    }

    /**
     * Returns currently available run names.
     */
    @Override
    public Collection<String> getRunNames() {
        return activeRunSet.getRunNames();
    }

    /**
     * Adds a run which is mapped for the specified name. Note: If the run was marked for uninstall via
     * {@link #uninstallRun(String)}, it should continue to be added across scheduler restarts in order for uninstall to
     * complete. It should only be omitted after the uninstall callback has been invoked for it.
     *
     * @param run the client to add
     * @return {@code this}
     * @throws IllegalArgumentException if the run name is already present
     */
    @Override
    public DefaultRunManager putRun(AbstractScheduler run) {
        Map<String, AbstractScheduler> runs = activeRunSet.lockRW();
        String runServiceName = run.getServiceSpec().getName();
        LOGGER.info("Adding service: {} (now {} services)", runServiceName, runs.size() + 1);
        try {
            // NOTE: If the run is uninstalling, it should already be passed to us as an UninstallScheduler.
            // See SchedulerBuilder.
            AbstractScheduler previousRun = runs.put(runServiceName, run);
            if (previousRun != null) {
                // Put the old client back before throwing...
                runs.put(runServiceName, previousRun);
                throw new IllegalArgumentException(
                        String.format("Service named '%s' is already present", runServiceName));
            }
            if (isRegistered) {
                // We are already registered. Manually call registered() against this client so that it can initialize.
                run.registered(false);
            }
            return this;
        } finally {
            activeRunSet.unlockRW();
        }
    }

    /**
     * Returns the specified run, or an empty {@code Optional} if it's not found.
     */
    @Override
    public Optional<AbstractScheduler> getRun(String runName) {
        return activeRunSet.getRun(runName);
    }

    /**
     * Triggers an uninstall for a run, removing it from the list of runs when it has finished. Does nothing if the run
     * is already uninstalling or doesn't exist. If the scheduler process is restarted, the run must be added again via
     * {@link #putRun(AbstractScheduler)}, at which point it will automatically resume uninstalling.
     *
     * @param name the name of the run to be uninstalled
     */
    @Override
    public void uninstallRun(String name) {
        uninstallRuns(Collections.singleton(name));
    }

    /****
     * The following calls are used by QueueEventClient only.
     ****/

    /**
     * Returns a RunInfoProvider which will provide information about underlying runs to HTTP endpoints.
     */
    public RunInfoProvider getRunInfoProvider() {
        return new RunInfoProvider(activeRunSet);
    }

    /**
     * UNINSTALL FLOW:
     * 1. uninstallRun("foo") is called. This converts the run to an UninstallScheduler.
     * 2. UninstallScheduler internally flags its StateStore with an uninstall bit if one is not already present.
     * 3. The UninstallScheduler proceeds to clean up the service.
     * 4. In the event of a scheduler process restart during cleanup:
     *   a. Upstream builds a new foo using SchedulerBuilder, which internally finds the uninstall bit and returns a
     *      new UninstallScheduler
     *   b. putRun(foo) is called with the UninstallScheduler
     *   c. The UninstallScheduler resumes cleanup from where it left off...
     * 5. Sometime after the UninstallScheduler finishes cleanup, it returns FINISHED in response to offers.
     * 6. We remove the run and invoke uninstallCallback.uninstalled(), telling upstream that it's gone. If upstream
     *    invokes putRun(foo) again at this point, the run will be relaunched from scratch because the uninstall bit
     *    in ZK will have been cleared.
     */
    public void uninstallRuns(Collection<String> finishedRunNames) {
        Map<String, AbstractScheduler> runs = activeRunSet.lockRW();
        LOGGER.info("Marking services as uninstalling: {} (out of {} services)", finishedRunNames, runs.size());
        try {
            for (String name : finishedRunNames) {
                AbstractScheduler currentRun = runs.get(name);
                if (currentRun == null) {
                    LOGGER.warn("Service '{}' does not exist, cannot trigger uninstall", name);
                    continue;
                }
                if (currentRun instanceof UninstallScheduler) {
                    // Already uninstalling
                    LOGGER.warn("Service '{}' is already uninstalling, leaving as-is", name);
                    continue;
                }

                // Convert the DefaultScheduler to an UninstallScheduler. It will automatically flag itself with an
                // uninstall bit in its state store and then proceed with the uninstall. When the uninstall has
                // completed, it will return FINISHED to its next offers() call, at which point we will remove it. If
                // the scheduler process is restarted before uninstall has completed, the caller should have added it
                // back via putRun(). When it's added back, it should be have already been converted to an
                // UninstallScheduler. See SchedulerBuilder.
                AbstractScheduler uninstallScheduler = ((DefaultScheduler) currentRun).toUninstallScheduler();
                if (isRegistered) {
                    // Don't forget to also initialize the new scheduler if relevant...
                    uninstallScheduler.registered(false);
                }
                runs.put(name, uninstallScheduler);
            }
        } finally {
            activeRunSet.unlockRW();
        }
    }

    /**
     * Removes the specified runs after they have completed uninstall. Any unknown run names are ignored.
     *
     * @return the number of runs which are still present after this removal
     */
    public int removeRuns(Collection<String> uninstalledRunNames) {
        Map<String, AbstractScheduler> runsMap = activeRunSet.lockRW();
        LOGGER.info("Removing {} uninstalled service{}: {} (from {} total services)",
                uninstalledRunNames.size(),
                uninstalledRunNames.size() == 1 ? "" : "s",
                uninstalledRunNames,
                runsMap.size());
        try {
            for (String run : uninstalledRunNames) {
                runsMap.remove(run);
            }
            return runsMap.size();
        } finally {
            activeRunSet.unlockRW();
        }
    }

    /**
     * Gets a shared/read lock on the underlying data store, and returns all available runs.
     * Upstream MUST call {@link #unlockRuns()} after finishing.
     */
    public Collection<AbstractScheduler> lockAndGetRuns() {
        return activeRunSet.lockR();
    }

    /**
     * Unlocks a previous lock which was obtained via {@link #lockAndGetRuns()}.
     */
    public void unlockRuns() {
        activeRunSet.unlockR();
    }

    /**
     * Notifies underlying runs that a registration or re-registration has occurred. After this has been invoked, any
     * runs which are added in the future will automatically have their {@code registered()} call invoked to reflect
     * that registration has already occurred.
     */
    public void registered(boolean reRegistered) {
        // Take exclusive lock in order to lock against clients being added/removed, ensuring our hasRegistered handling
        // behaves consistently:
        Collection<AbstractScheduler> runs = activeRunSet.lockRW().values();
        isRegistered = true;
        LOGGER.info("Notifying {} services of {}",
                runs.size(), reRegistered ? "re-registration" : "initial registration");
        try {
            runs.stream().forEach(c -> c.registered(reRegistered));
        } finally {
            activeRunSet.unlockRW();
        }
    }
}
