package com.mesosphere.sdk.queues.http.endpoints;

import com.mesosphere.sdk.http.queries.StateQueries;
import com.mesosphere.sdk.http.types.PropertyDeserializer;
import com.mesosphere.sdk.queues.http.types.RunInfoProvider;
import com.mesosphere.sdk.state.StateStore;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.*;
import java.util.Optional;

/**
 * An API for reading task and frameworkId state from persistent storage, and resetting the state store cache if one is
 * being used.
 */
@Path("/v1/runs")
public class RunsStateResource {

    private final RunInfoProvider runInfoProvider;
    private final PropertyDeserializer propertyDeserializer;

    /**
     * Creates a new StateResource which returns content for runs in the provider.
     */
    public RunsStateResource(RunInfoProvider runInfoProvider, PropertyDeserializer propertyDeserializer) {
        this.runInfoProvider = runInfoProvider;
        this.propertyDeserializer = propertyDeserializer;
    }

    /**
     * @see StateQueries
     */
    @Path("{runName}/state/files")
    @Produces(MediaType.TEXT_PLAIN)
    @GET
    public Response getFiles(@PathParam("runName") String runName) {
        Optional<StateStore> stateStore = runInfoProvider.getStateStore(runName);
        if (!stateStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return StateQueries.getFiles(stateStore.get());
    }

    /**
     * @see StateQueries
     */
    @Path("{runName}/state/files/{file}")
    @Produces(MediaType.TEXT_PLAIN)
    @GET
    public Response getFile(@PathParam("runName") String runName, @PathParam("file") String fileName) {
        Optional<StateStore> stateStore = runInfoProvider.getStateStore(runName);
        if (!stateStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return StateQueries.getFile(stateStore.get(), fileName);
    }

    /**
     * @see StateQueries
     */
    @Path("{runName}/state/files/{file}")
    @PUT
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response putFile(
            @PathParam("runName") String runName,
            @FormDataParam("file") InputStream uploadedInputStream,
            @FormDataParam("file") FormDataContentDisposition fileDetails) {
        Optional<StateStore> stateStore = runInfoProvider.getStateStore(runName);
        if (!stateStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return StateQueries.putFile(stateStore.get(), uploadedInputStream, fileDetails);
    }

    /**
     * @see StateQueries
     */
    @Path("{runName}/state/zone/tasks")
    @GET
    public Response getTaskNamesToZones(@PathParam("runName") String runName) {
        Optional<StateStore> stateStore = runInfoProvider.getStateStore(runName);
        if (!stateStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return StateQueries.getTaskNamesToZones(stateStore.get());
    }

    /**
     * @see StateQueries
     */
    @Path("{runName}/state/zone/tasks/{taskName}")
    @GET
    public Response getTaskNameToZone(@PathParam("runName") String runName, @PathParam("taskName") String taskName) {
        Optional<StateStore> stateStore = runInfoProvider.getStateStore(runName);
        if (!stateStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return StateQueries.getTaskNameToZone(stateStore.get(), taskName);
    }

    /**
     * @see StateQueries
     */
    @Path("{runName}/state/zone/{podType}/{ip}")
    @GET
    public Response getTaskIPsToZones(
            @PathParam("runName") String runName, @PathParam("podType") String podType, @PathParam("ip") String ip) {
        Optional<StateStore> stateStore = runInfoProvider.getStateStore(runName);
        if (!stateStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return StateQueries.getTaskIPsToZones(stateStore.get(), podType, ip);
    }

    /**
     * @see StateQueries
     */
    @Path("{runName}/state/properties")
    @GET
    public Response getPropertyKeys(@PathParam("runName") String runName) {
        Optional<StateStore> stateStore = runInfoProvider.getStateStore(runName);
        if (!stateStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return StateQueries.getPropertyKeys(stateStore.get());
    }

    /**
     * @see StateQueries
     */
    @Path("{runName}/state/properties/{key}")
    @GET
    public Response getProperty(@PathParam("runName") String runName, @PathParam("key") String key) {
        Optional<StateStore> stateStore = runInfoProvider.getStateStore(runName);
        if (!stateStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return StateQueries.getProperty(stateStore.get(), propertyDeserializer, key);
    }

    /**
     * @see StateQueries
     */
    @Path("{runName}/state/refresh")
    @PUT
    public Response refreshCache(@PathParam("runName") String runName) {
        Optional<StateStore> stateStore = runInfoProvider.getStateStore(runName);
        if (!stateStore.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return StateQueries.refreshCache(stateStore.get());
    }
}
