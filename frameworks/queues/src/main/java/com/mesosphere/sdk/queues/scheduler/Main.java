package com.mesosphere.sdk.queues.scheduler;

import com.mesosphere.sdk.queues.http.endpoints.RunsArtifactResource;
import com.mesosphere.sdk.scheduler.DefaultScheduler;
import com.mesosphere.sdk.scheduler.EnvStore;
import com.mesosphere.sdk.scheduler.FrameworkConfig;
import com.mesosphere.sdk.scheduler.SchedulerConfig;
import com.mesosphere.sdk.specification.DefaultServiceSpec;
import com.mesosphere.sdk.specification.ServiceSpec;
import com.mesosphere.sdk.specification.yaml.RawServiceSpec;

import java.io.File;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point for the Queues service.
 */
public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalArgumentException("Expected at least one file argument, got: " + Arrays.toString(args));
        }

        EnvStore envStore = EnvStore.fromEnv();
        SchedulerConfig schedulerConfig = SchedulerConfig.fromEnvStore(envStore);
        FrameworkConfig frameworkConfig = FrameworkConfig.fromEnvStore(envStore);
        RunsEventClient client = new RunsEventClient(schedulerConfig, new RunsEventClient.UninstallCallback() {
            @Override
            public void uninstalled(String name) {
                LOGGER.info("Job has completed uninstall: {}", name);
            }
        });

        // First initialize the QueueRunner to get the (cached) persister that will be reused by individual jobs
        // (within their own namespaces)
        // Note: In practice, jobs would be added over HTTP after run() begins
        QueueRunner.Builder runnerBuilder = QueueRunner.newBuilder(schedulerConfig, frameworkConfig, client);
        // Need to tell Mesos up-front whether we want GPU resources:
        if (envStore.getOptionalBoolean("FRAMEWORK_GPUS", false)) {
            runnerBuilder.enableGpus();
        }
        QueueRunner queueRunner = runnerBuilder.build();

        // Read jobs from provided files, and assume any config templates are in the same directory as the files:
        for (int i = 0; i < args.length; ++i) {
            LOGGER.info("Reading job from spec file: {}", args[i]);
            File yamlSpecFile = new File(args[i]);
            RawServiceSpec rawServiceSpec = RawServiceSpec.newBuilder(yamlSpecFile).build();
            ServiceSpec serviceSpec =
                    DefaultServiceSpec.newGenerator(rawServiceSpec, schedulerConfig, yamlSpecFile.getParentFile())
                    .setMultiServiceFrameworkConfig(frameworkConfig)
                    .build();
            LOGGER.info("Adding job: {}", serviceSpec.getName());
            client.putRun(DefaultScheduler.newBuilder(serviceSpec, schedulerConfig, queueRunner.getPersister())
                    .setPlansFrom(rawServiceSpec)
                    // Jobs-related customizations:
                    // - In ZK, store data under "dcos-service-<fwkName>/Services/<jobName>"
                    .setStorageNamespace(serviceSpec.getName())
                    // - Config templates are served by JobsArtifactResource rather than default ArtifactResource
                    .setTemplateUrlFactory(RunsArtifactResource.getUrlFactory(
                            frameworkConfig.getFrameworkName(), serviceSpec.getName()))
                    // If the service was previously marked for uninstall, it will be built as an UninstallScheduler
                    .build());
        }

        // Now run the queue.
        queueRunner.run();
    }
}
