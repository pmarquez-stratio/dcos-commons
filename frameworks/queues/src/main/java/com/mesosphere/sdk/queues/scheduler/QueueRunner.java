package com.mesosphere.sdk.queues.scheduler;

import com.mesosphere.sdk.curator.CuratorLocker;
import com.mesosphere.sdk.curator.CuratorPersister;
import com.mesosphere.sdk.scheduler.FrameworkConfig;
import com.mesosphere.sdk.scheduler.FrameworkRunner;
import com.mesosphere.sdk.scheduler.MesosEventClient;
import com.mesosphere.sdk.scheduler.Metrics;
import com.mesosphere.sdk.scheduler.SchedulerConfig;
import com.mesosphere.sdk.scheduler.ServiceScheduler;
import com.mesosphere.sdk.storage.Persister;
import com.mesosphere.sdk.storage.PersisterCache;
import com.mesosphere.sdk.storage.PersisterException;

/**
 * Sets up and executes a {@link FrameworkRunner} to which potentially multiple {@link ServiceScheduler}s may be added.
 *
 * <p>WARNING: This is not a stable API, and can go away at any time.
 */
public class QueueRunner implements Runnable {

    private final SchedulerConfig schedulerConfig;
    private final FrameworkConfig frameworkConfig;
    private final Persister persister;
    private final MesosEventClient client;
    private final boolean usingGpus;

    /**
     * Returns a new {@link QueueRunner} instance which may be launched with {@code run()}.
     *
     * @param client the Mesos event client which receives offers/statuses from Mesos. Note that this may route events
     *               to multiple wrapped clients
     */
    public static QueueRunner build(
            SchedulerConfig schedulerConfig,
            FrameworkConfig frameworkConfig,
            MesosEventClient client,
            boolean usingGpus) {
        Persister persister;
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

        // Lock curator before returning access to persister.
        CuratorLocker.lock(frameworkConfig.getFrameworkName(), frameworkConfig.getZookeeperHostPort());

        return new QueueRunner(schedulerConfig, frameworkConfig, persister, client, usingGpus);
    }

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
