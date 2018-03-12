package com.mesosphere.sdk.queues.scheduler;

import com.mesosphere.sdk.curator.CuratorPersister;
import com.mesosphere.sdk.framework.FrameworkConfig;
import com.mesosphere.sdk.queues.generator.Generator;
import com.mesosphere.sdk.queues.generator.SDKYamlGenerator;
import com.mesosphere.sdk.queues.state.SpecStore;
import com.mesosphere.sdk.scheduler.AbstractScheduler;
import com.mesosphere.sdk.scheduler.EnvStore;
import com.mesosphere.sdk.scheduler.SchedulerConfig;
import com.mesosphere.sdk.storage.Persister;
import com.mesosphere.sdk.storage.PersisterCache;
import com.mesosphere.sdk.storage.PersisterException;

import java.io.File;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for the Queues service.
 */
public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        EnvStore envStore = EnvStore.fromEnv();
        SchedulerConfig schedulerConfig = SchedulerConfig.fromEnvStore(envStore);
        FrameworkConfig frameworkConfig = FrameworkConfig.fromEnvStore(envStore);

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

        DefaultRunManager runManager = new DefaultRunManager(new ActiveRunSet());

        // Generators which will convert submitted payloads into ServiceSpecs
        final Map<String, Generator> generators = new HashMap<>();
        // TODO(nickbp): generators.put("spark", new SparkGenerator());
        generators.put("yaml", new SDKYamlGenerator(
                schedulerConfig,
                frameworkConfig,
                persister,
                // Config template path: just use current working directory
                new File(Paths.get("").toAbsolutePath().toString())));
        // If the user doesn't include a 'type' value when submitting a spec, assume they mean 'yaml':
        Optional<String> defaultSpecType = Optional.of("yaml");

        // Recover any persisted runs that were active before we had restarted.
        // This includes any runs which were in the process of uninstalling.
        SpecStore specStore = new SpecStore(persister);
        Collection<AbstractScheduler> recoveredRuns = specStore.recover(generators);
        if (!recoveredRuns.isEmpty()) {
            LOGGER.info("Recovering {} runs from persistent storage: {}",
                    recoveredRuns.size(),
                    recoveredRuns.stream()
                            .map(r -> r.getServiceSpec().getName())
                            .sorted()
                            .collect(Collectors.toList()));
            for (AbstractScheduler recoveredRun : recoveredRuns) {
                runManager.putRun(recoveredRun);
            }
        }

        // Client which will receive events from mesos and forward them to active Runs
        QueueEventClient client = new QueueEventClient(
                schedulerConfig,
                specStore,
                runManager,
                generators,
                defaultSpecType,
                new QueueEventClient.UninstallCallback() {
            @Override
            public void uninstalled(String name) {
                // TODO(nickbp): This would be a good spot to prune unused/dangling specs from the SpecStore.
                LOGGER.info("Job has completed uninstall: {}", name);
            }
        });
        QueueRunner.Builder queueRunnerBuilder =
                QueueRunner.newBuilder(schedulerConfig, frameworkConfig, persister, client);
        // Need to tell Mesos up-front whether we want GPU resources:
        if (envStore.getOptionalBoolean("FRAMEWORK_GPUS", false)) {
            queueRunnerBuilder.enableGpus();
        }
        queueRunnerBuilder.build().run();
    }
}
