package com.mesosphere.sdk.queues.http.endpoints;

import com.mesosphere.sdk.http.EndpointUtils;
import com.mesosphere.sdk.http.queries.ArtifactQueries;
import com.mesosphere.sdk.queues.http.types.RunInfoProvider;
import com.mesosphere.sdk.specification.ServiceSpec;
import com.mesosphere.sdk.state.ConfigStore;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import java.util.Optional;
import java.util.UUID;

/**
 * A read-only API for accessing file artifacts (e.g. config templates) for retrieval by pods.
 */
@Path("/v1/run")
public class RunArtifactResource {

    private static final String RUN_ARTIFACT_URI_FORMAT = "http://%s/v1/runs/%s/artifacts/template/%s/%s/%s/%s";

    private final RunInfoProvider runInfoProvider;

    public RunArtifactResource(RunInfoProvider runInfoProvider) {
        this.runInfoProvider = runInfoProvider;
    }

    /**
     * Returns a factory for schedulers which use {@link RunArtifactResource}.
     *
     * @param frameworkName the name of the scheduler framework
     * @param serviceName the name of a run/service being managed by the scheduler
     */
    public static ArtifactQueries.TemplateUrlFactory getUrlFactory(String frameworkName, String serviceName) {
        String hostname = EndpointUtils.toSchedulerApiVipHostname(frameworkName);
        return new ArtifactQueries.TemplateUrlFactory() {
            @Override
            public String get(UUID configId, String podType, String taskName, String configName) {
                return String.format(
                        RUN_ARTIFACT_URI_FORMAT, hostname, serviceName, configId, podType, taskName, configName);
            }
        };
    }

    /**
     * @see ArtifactQueries
     */
    @Path("{runName}/artifacts/template/{configurationId}/{podType}/{taskName}/{configurationName}")
    @GET
    public Response getTemplate(
            @PathParam("runName") String runName,
            @PathParam("configurationId") String configurationId,
            @PathParam("podType") String podType,
            @PathParam("taskName") String taskName,
            @PathParam("configurationName") String configurationName) {
        Optional<ConfigStore<ServiceSpec>> configStore = runInfoProvider.getConfigStore(runName);
        if (!configStore.isPresent()) {
            return QueueResponseUtils.runNotFoundResponse(runName);
        }
        return ArtifactQueries.getTemplate(configStore.get(), configurationId, podType, taskName, configurationName);
    }
}
