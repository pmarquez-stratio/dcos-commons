package com.mesosphere.sdk.queues.scheduler;

import com.mesosphere.sdk.curator.CuratorPersister;
import com.mesosphere.sdk.framework.FrameworkConfig;
import com.mesosphere.sdk.queues.generator.RunGenerator;
import com.mesosphere.sdk.queues.generator.SDKYamlGenerator;
import com.mesosphere.sdk.scheduler.EnvStore;
import com.mesosphere.sdk.scheduler.SchedulerConfig;
import com.mesosphere.sdk.storage.Persister;
import com.mesosphere.sdk.storage.PersisterCache;
import com.mesosphere.sdk.storage.PersisterException;

import java.io.File;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

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
        final Map<String, RunGenerator> runGenerators = new HashMap<>();
        //runGenerators.put("spark", new SparkGenerator());
        runGenerators.put("yaml", new SDKYamlGenerator(
                schedulerConfig,
                frameworkConfig,
                persister,
                // Config template path: just use current working directory
                new File(Paths.get("").toAbsolutePath().toString())));

        // Client which will receive events from mesos and forward them to active Runs
        QueueEventClient client = new QueueEventClient(
                schedulerConfig,
                runManager,
                runGenerators,
                new QueueEventClient.UninstallCallback() {
            @Override
            public void uninstalled(String name) {
                // TODO(nickbp): Remove run from storage
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
