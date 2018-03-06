package com.mesosphere.sdk.queues.http.endpoints;

import com.mesosphere.sdk.http.queries.PodQueries;
import com.mesosphere.sdk.http.types.PrettyJsonResource;
import com.mesosphere.sdk.queues.http.types.RunInfoProvider;
import com.mesosphere.sdk.scheduler.recovery.RecoveryType;
import com.mesosphere.sdk.specification.ServiceSpec;
import com.mesosphere.sdk.state.ConfigStore;
import com.mesosphere.sdk.state.StateStore;

import java.util.Optional;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

/**
 * A read-only API for accessing information about the pods which compose the service, and restarting/replacing those
 * pods.
 */
@Path("/v1/runs")
public class RunsPodResource extends PrettyJsonResource {

    private final RunInfoProvider runInfoProvider;

    /**
     * Creates a new instance which retrieves task/pod state from the provided {@link StateStore}.
     */
    public RunsPodResource(RunInfoProvider runInfoProvider) {
        this.runInfoProvider = runInfoProvider;
    }

    /**
     * @see PodQueries
     */
    @Path("{runName}/pod")
    @GET
    public Response list(@PathParam("runName") String runName) {
        Optional<StateStore> stateStore = runInfoProvider.getStateStore(runName);
        if (!stateStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return PodQueries.list(stateStore.get());
    }

    /**
     * @see PodQueries
     */
    @Path("{runName}/pod/status")
    @GET
    public Response getStatuses(@PathParam("runName") String runName) {
        Optional<StateStore> stateStore = runInfoProvider.getStateStore(runName);
        if (!stateStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return PodQueries.getStatuses(stateStore.get(), runName);
    }

    /**
     * @see PodQueries
     */
    @Path("{runName}/pod/{name}/status")
    @GET
    public Response getStatus(@PathParam("runName") String runName, @PathParam("name") String podInstanceName) {
        Optional<StateStore> stateStore = runInfoProvider.getStateStore(runName);
        if (!stateStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return PodQueries.getStatus(stateStore.get(), podInstanceName);
    }

    /**
     * @see PodQueries
     */
    @Path("{runName}/pod/{name}/info")
    @GET
    public Response getInfo(@PathParam("runName") String runName, @PathParam("name") String podInstanceName) {
        Optional<StateStore> stateStore = runInfoProvider.getStateStore(runName);
        if (!stateStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return PodQueries.getInfo(stateStore.get(), podInstanceName);
    }

    /**
     * @see PodQueries
     */
    @Path("{runName}/pod/{name}/pause")
    @POST
    public Response pause(
            @PathParam("runName") String runName, @PathParam("name") String podInstanceName, String bodyPayload) {
        Optional<StateStore> stateStore = runInfoProvider.getStateStore(runName);
        if (!stateStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return PodQueries.pause(stateStore.get(), podInstanceName, bodyPayload);
    }

    /**
     * @see PodQueries
     */
    @Path("{runName}/pod/{name}/resume")
    @POST
    public Response resume(
            @PathParam("runName") String runName, @PathParam("name") String podInstanceName, String bodyPayload) {
        Optional<StateStore> stateStore = runInfoProvider.getStateStore(runName);
        if (!stateStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return PodQueries.resume(stateStore.get(), podInstanceName, bodyPayload);
    }

    /**
     * @see PodQueries
     */
    @Path("{runName}/pod/{name}/restart")
    @POST
    public Response restart(@PathParam("runName") String runName, @PathParam("name") String podInstanceName) {
        return restart(runName, podInstanceName, RecoveryType.TRANSIENT);
    }

    /**
     * @see PodQueries
     */
    @Path("{runName}/pod/{name}/replace")
    @POST
    public Response replace(@PathParam("runName") String runName, @PathParam("name") String podInstanceName) {
        return restart(runName, podInstanceName, RecoveryType.PERMANENT);
    }

    private Response restart(String runName, String podInstanceName, RecoveryType recoveryType) {
        Optional<StateStore> stateStore = runInfoProvider.getStateStore(runName);
        if (!stateStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        Optional<ConfigStore<ServiceSpec>> configStore = runInfoProvider.getConfigStore(runName);
        if (!configStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return PodQueries.restart(stateStore.get(), configStore.get(), podInstanceName, recoveryType);
    }
}
