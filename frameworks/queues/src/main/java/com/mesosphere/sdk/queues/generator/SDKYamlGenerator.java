package com.mesosphere.sdk.queues.generator;

import java.io.File;

import com.mesosphere.sdk.framework.FrameworkConfig;
import com.mesosphere.sdk.queues.http.endpoints.RunArtifactResource;
import com.mesosphere.sdk.scheduler.AbstractScheduler;
import com.mesosphere.sdk.scheduler.DefaultScheduler;
import com.mesosphere.sdk.scheduler.SchedulerConfig;
import com.mesosphere.sdk.specification.DefaultServiceSpec;
import com.mesosphere.sdk.specification.ServiceSpec;
import com.mesosphere.sdk.specification.yaml.RawServiceSpec;
import com.mesosphere.sdk.storage.Persister;

/**
 * Default implementation of {@link Generator} with support for run types in the default queue service.
 */
public class SDKYamlGenerator implements Generator {

    // 64k ought to be good enough for anybody.
    private static final int PAYLOAD_SIZE_LIMIT = 65536;

    private final SchedulerConfig schedulerConfig;
    private final FrameworkConfig frameworkConfig;
    private final Persister persister;
    private final File configTemplateDir;

    public SDKYamlGenerator(
            SchedulerConfig schedulerConfig,
            FrameworkConfig frameworkConfig,
            Persister persister,
            File configTemplateDir) {
        this.schedulerConfig = schedulerConfig;
        this.frameworkConfig = frameworkConfig;
        this.persister = persister;
        this.configTemplateDir = configTemplateDir;
    }

    @Override
    public AbstractScheduler generate(byte[] data) throws Exception {
        RawServiceSpec rawServiceSpec = RawServiceSpec.fromBytes(data);
        ServiceSpec serviceSpec =
                DefaultServiceSpec.newGenerator(rawServiceSpec, schedulerConfig, configTemplateDir)
                .setMultiServiceFrameworkConfig(frameworkConfig)
                .build();
        return DefaultScheduler.newBuilder(serviceSpec, schedulerConfig, persister)
                .setPlansFrom(rawServiceSpec)
                // Jobs-related customizations:
                // - In ZK, store data under "dcos-service-<fwkName>/Services/<jobName>"
                .setStorageNamespace(serviceSpec.getName())
                // - Config templates are served by RunArtifactResource rather than default ArtifactResource
                .setTemplateUrlFactory(RunArtifactResource.getUrlFactory(
                        frameworkConfig.getFrameworkName(), serviceSpec.getName()))
                // If the service was previously marked for uninstall, it will be built as an UninstallScheduler
                .build();
    }

    @Override
    public int getMaxDataSizeBytes() {
        return PAYLOAD_SIZE_LIMIT;
    }
}
