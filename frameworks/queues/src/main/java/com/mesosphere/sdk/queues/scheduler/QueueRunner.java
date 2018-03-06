package com.mesosphere.sdk.queues.scheduler;

import com.mesosphere.sdk.curator.CuratorLocker;
import com.mesosphere.sdk.curator.CuratorPersister;
import com.mesosphere.sdk.scheduler.FrameworkConfig;
import com.mesosphere.sdk.scheduler.FrameworkRunner;
import com.mesosphere.sdk.scheduler.MesosEventClient;
import com.mesosphere.sdk.scheduler.Metrics;
import com.mesosphere.sdk.scheduler.SchedulerConfig;
import com.mesosphere.sdk.scheduler.ServiceScheduler;
import com.mesosphere.sdk.state.SchemaVersionStore;
import com.mesosphere.sdk.storage.Persister;
import com.mesosphere.sdk.storage.PersisterCache;
import com.mesosphere.sdk.storage.PersisterException;

/**
 * Sets up and executes a {@link FrameworkRunner} to which potentially multiple {@link ServiceScheduler}s may be added.
 *
 * <p>WARNING: This is not a stable API, and can go away at any time.
 */
public class QueueRunner implements Runnable {

    /**
     * Builder for {@link QueueRunner}.
     */
    public static class Builder {
        private final SchedulerConfig schedulerConfig;
        private final FrameworkConfig frameworkConfig;
        private final MesosEventClient client;

        private Persister persister;
        private boolean usingGpus = false;

        private Builder(SchedulerConfig schedulerConfig, FrameworkConfig frameworkConfig, MesosEventClient client) {
            this.schedulerConfig = schedulerConfig;
            this.frameworkConfig = frameworkConfig;
            this.client = client;
        }

        /**
         * Tells Mesos that we want to be offered GPU resources.
         *
         * @return {@code this}
         */
        public Builder enableGpus() {
            this.usingGpus = true;
            return this;
        }

        /**
         * Overrides the persister to be used, intended for testing.
         *
         * @return {@code this}
         */
        public Builder setCustomPersister(Persister persister) {
            this.persister = persister;
            return this;
        }

        /**
         * Returns a new {@link QueueRunner} instance which may be launched with {@code run()}.
         */
        public QueueRunner build() {
            if (persister == null) {
                // Default: Curator persister
                try {
                    persister = CuratorPersister.newBuilder(
                            frameworkConfig.getFrameworkName(), frameworkConfig.getZookeeperHostPort()).build();
                    if (schedulerConfig.isStateCacheEnabled()) {
                        persister = new PersisterCache(persister);
                    }
                } catch (PersisterException e) {
                    throw new IllegalStateException(String.format(
                            "Failed to initialize default persister at %s for framework %s",
                            frameworkConfig.getZookeeperHostPort(), frameworkConfig.getFrameworkName()));
                }
            }

            // Lock curator before returning access to persister.
            CuratorLocker.lock(frameworkConfig.getFrameworkName(), frameworkConfig.getZookeeperHostPort());
            // Check and/or initialize schema version before doing any other storage access:
            new SchemaVersionStore(persister).check(SUPPORTED_SCHEMA_VERSION_MULTI_SERVICE);

            return new QueueRunner(schedulerConfig, frameworkConfig, persister, client, usingGpus);
        }
    }

    /**
     * Schema version used by single-service schedulers, which is what {@link QueueRunner} runs.
     */
    private static final int SUPPORTED_SCHEMA_VERSION_MULTI_SERVICE = 2;

    private final SchedulerConfig schedulerConfig;
    private final FrameworkConfig frameworkConfig;
    private final Persister persister;
    private final MesosEventClient client;
    private final boolean usingGpus;

    /**
     * Returns a new {@link Builder} instance which may be customize before building the {@link QueueRunner}.
     *
     * @param client the Mesos event client which receives offers/statuses from Mesos. Note that this may route
     *               events to multiple wrapped clients
     */
    public static Builder newBuilder(
            SchedulerConfig schedulerConfig,
            FrameworkConfig frameworkConfig,
            MesosEventClient client) {
        return new Builder(schedulerConfig, frameworkConfig, client);
    }

    /**
     * Returns a new {@link QueueRunner} instance which may be launched with {@code run()}.
     *
     * @param client the Mesos event client which receives offers/statuses from Mesos. Note that this may route events
     *               to multiple wrapped clients
     */
    private QueueRunner(
            SchedulerConfig schedulerConfig,
            FrameworkConfig frameworkConfig,
            Persister persister,
            MesosEventClient client,
            boolean usingGpus) {
        this.schedulerConfig = schedulerConfig;
        this.frameworkConfig = frameworkConfig;
        this.persister = persister;
        this.client = client;
        this.usingGpus = usingGpus;
    }

    /**
     * Returns the persister which should be passed to individual jobs.
     */
    public Persister getPersister() {
        return persister;
    }

    /**
     * Runs the queue. Don't forget to call this!
     * This should never exit, instead the entire process will be terminated internally.
     */
    @Override
    public void run() {
        Metrics.configureStatsd(schedulerConfig);
        new FrameworkRunner(schedulerConfig, frameworkConfig, usingGpus).registerAndRunFramework(persister, client);
    }
}
