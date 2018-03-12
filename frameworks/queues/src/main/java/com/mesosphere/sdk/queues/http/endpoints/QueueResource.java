package com.mesosphere.sdk.queues.http.endpoints;

import com.mesosphere.sdk.http.RequestUtils;
import com.mesosphere.sdk.http.ResponseUtils;
import com.mesosphere.sdk.offer.LoggingUtils;
import com.mesosphere.sdk.queues.generator.Generator;
import com.mesosphere.sdk.queues.http.types.RunManager;
import com.mesosphere.sdk.queues.state.SpecStore;
import com.mesosphere.sdk.scheduler.AbstractScheduler;
import com.mesosphere.sdk.scheduler.uninstall.UninstallScheduler;
import com.mesosphere.sdk.state.StateStoreException;

import java.io.InputStream;
import java.util.Map;
import java.util.Optional;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;

/**
 * An API for listing, submitting, and uninstalling runs against the queue.
 */
@Path("/v1/queue")
public class QueueResource {

    private static final Logger logger = LoggingUtils.getLogger(QueueResource.class);

    private final SpecStore specStore;
    private final RunManager runManager;
    private final Map<String, Generator> generators;
    private final Optional<String> defaultSpecType;

    /**
     * Returns a new QueueResource instance which serves endpoints for managing the queue.
     *
     * @param specStore the spec store which contains submitted specs for active and scheduled runs
     * @param runManager the manager of active runs
     * @param generators one or more generators for converting submitted specs to running services based on type
     * @param defaultSpecType an optional default spec type to be used if no type is specified in a request,
     *     or an empty optional if the type must always be provided
     */
    public QueueResource(
            SpecStore specStore,
            RunManager runManager,
            Map<String, Generator> generators,
            Optional<String> defaultSpecType) {
        this.specStore = specStore;
        this.runManager = runManager;
        this.generators = generators;
        this.defaultSpecType = defaultSpecType;
        if (defaultSpecType.isPresent() && !generators.containsKey(defaultSpecType.get())) {
            throw new IllegalArgumentException(String.format(
                    "Default spec type %s is not present in the generator types: %s",
                    defaultSpecType.get(), generators.keySet()));
        }
    }

    /**
     * Returns a list of active runs.
     */
    @GET
    public Response getRuns() {
        JSONArray runs = new JSONArray();
        for (String runName : runManager.getRunNames()) {
            JSONObject run = new JSONObject();
            run.put("name", runName);

            // Try to get the scheduler, then the run id from the scheduler:
            Optional<AbstractScheduler> scheduler = runManager.getRun(runName);
            // Technically, the scheduler could disappear if it's uninstalled while we iterate over runNames.
            if (!scheduler.isPresent()) {
                continue;
            }
            // Goal state
            run.put("goal", scheduler.get().getServiceSpec().getGoal().toString());
            // Spec id (should be retrievable, but just in case...)
            Optional<String> specId = specStore.getSpecId(scheduler.get());
            if (specId.isPresent()) {
                run.put("spec-id", specId.get());
            }
            // Detect uninstall-in-progress by class type
            run.put("uninstall", scheduler.get() instanceof UninstallScheduler);

            runs.put(run);
        }
        return ResponseUtils.jsonOkResponse(runs);
    }

    /**
     * Triggers uninstall of a specified run.
     */
    @Path("{runName}")
    @DELETE
    public Response uninstallRun(@PathParam("runName") String runName) {
        try {
            runManager.uninstallRun(runName);
            return ResponseUtils.plainOkResponse("Triggered removal of run: " + runName);
        } catch (IllegalArgumentException e) {
            return QueueResponseUtils.runNotFoundResponse(runName);
        }
    }

    /**
     * Accepts a new run of a specified type to be launched immediately.
     *
     * Parameters must be passed as form data. In {@code curl} this would be something like:
     * {@code curl -X POST -F 'type=foo' -F 'file=@filename.txt' ...}
     */
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response submitRun(
            @FormDataParam("type") String type,
            @FormDataParam("file") InputStream uploadedInputStream,
            @FormDataParam("file") FormDataContentDisposition fileDetails) {
        // Use default type if applicable
        if (type == null && defaultSpecType.isPresent()) {
            type = defaultSpecType.get();
        }

        // Select the matching generator.
        final Generator runGenerator = generators.get(type);
        if (runGenerator == null) {
            // Bad type.
            return ResponseUtils.plainResponse(
                    String.format("Invalid 'type' value '%s'. Must be one of: %s", type, generators.keySet()),
                    Response.Status.BAD_REQUEST);
        }

        // Extract the data payload.
        byte[] data;
        try {
            data = RequestUtils.readData(uploadedInputStream, fileDetails, runGenerator.getMaxDataSizeBytes());
        } catch (IllegalArgumentException e) {
            // Size limit exceeded or other user input error
            return ResponseUtils.plainResponse(e.getMessage(), Response.Status.BAD_REQUEST);
        } catch (Exception e) {
            logger.error("Failed to read payload", e);
            return Response.serverError().build();
        }

        // Generate the run of the specified type using the provided payload.
        AbstractScheduler run;
        try {
            run = runGenerator.generate(data);
            // Service was generated successfully. Store the data we used to perform the generation, so that we can
            // restore this service if we are later restarted.
            specStore.store(run.getStateStore(), data, type);
        } catch (Exception e) {
            logger.error("Failed to generate service", e);
            return ResponseUtils.plainResponse(
                    String.format("Failed to generate service from provided payload: %s", e.getMessage()),
                    Response.Status.BAD_REQUEST);
        }

        try {
            runManager.putRun(run);
            JSONObject obj = new JSONObject();
            obj.put("name", run.getServiceSpec().getName());
            return ResponseUtils.jsonOkResponse(obj);
        } catch (Exception e) {
            logger.error(String.format("Failed to add run %s", run.getServiceSpec().getName()), e);
            try {
                // Wipe the data that we just added against this service
                run.getStateStore().deleteAllDataIfNamespaced();
            } catch (StateStoreException e2) {
                logger.error(
                        String.format("Failed to clear service data for failed run %s", run.getServiceSpec().getName()),
                        e2);
            }
            return ResponseUtils.plainResponse(
                    String.format("Failed to add run: %s", e.getMessage()), Response.Status.BAD_REQUEST);
        }
    }
}
