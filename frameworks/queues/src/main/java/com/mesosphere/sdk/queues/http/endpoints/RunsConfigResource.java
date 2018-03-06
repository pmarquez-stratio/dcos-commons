package com.mesosphere.sdk.queues.http.endpoints;

import java.util.Optional;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import com.mesosphere.sdk.http.queries.ConfigQueries;
import com.mesosphere.sdk.http.types.PrettyJsonResource;
import com.mesosphere.sdk.queues.http.types.RunInfoProvider;
import com.mesosphere.sdk.specification.ServiceSpec;
import com.mesosphere.sdk.state.ConfigStore;

/**
 * A read-only API for accessing active and inactive configurations from persistent storage.
 */
@Path("/v1/runs")
public class RunsConfigResource extends PrettyJsonResource {

    private final RunInfoProvider runInfoProvider;

    public RunsConfigResource(RunInfoProvider runInfoProvider) {
        this.runInfoProvider = runInfoProvider;
    }

    /**
     * @see ConfigQueries
     */
    @Path("{runName}/configurations")
    @GET
    public Response getConfigurationIds(@PathParam("runName") String runName) {
        Optional<ConfigStore<ServiceSpec>> configStore = runInfoProvider.getConfigStore(runName);
        if (!configStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return ConfigQueries.<ServiceSpec>getConfigurationIds(configStore.get());
    }

    /**
     * @see ConfigQueries
     */
    @Path("{runName}/configurations/{configurationId}")
    @GET
    public Response getConfiguration(
            @PathParam("runName") String runName, @PathParam("configurationId") String configurationId) {
        Optional<ConfigStore<ServiceSpec>> configStore = runInfoProvider.getConfigStore(runName);
        if (!configStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return ConfigQueries.<ServiceSpec>getConfiguration(configStore.get(), configurationId);
    }

    /**
     * @see ConfigQueries
     */
    @Path("{runName}/configurations/targetId")
    @GET
    public Response getTargetId(@PathParam("runName") String runName) {
        Optional<ConfigStore<ServiceSpec>> configStore = runInfoProvider.getConfigStore(runName);
        if (!configStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return ConfigQueries.getTargetId(configStore.get());
    }

    /**
     * @see ConfigQueries
     */
    @Path("{runName}/configurations/target")
    @GET
    public Response getTarget(@PathParam("runName") String runName) {
        Optional<ConfigStore<ServiceSpec>> configStore = runInfoProvider.getConfigStore(runName);
        if (!configStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return ConfigQueries.getTarget(configStore.get());
    }
}
