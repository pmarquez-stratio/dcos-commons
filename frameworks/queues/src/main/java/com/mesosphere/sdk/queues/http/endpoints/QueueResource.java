package com.mesosphere.sdk.queues.http.endpoints;

import com.mesosphere.sdk.http.RequestUtils;
import com.mesosphere.sdk.http.ResponseUtils;
import com.mesosphere.sdk.offer.LoggingUtils;
import com.mesosphere.sdk.queues.generator.RunGenerator;
import com.mesosphere.sdk.queues.http.types.RunManager;
import com.mesosphere.sdk.scheduler.AbstractScheduler;

import java.io.InputStream;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.StringUtils;
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

    private final RunManager runManager;
    private final Map<String, RunGenerator> runGenerators;

    public QueueResource(RunManager runManager, Map<String, RunGenerator> specGenerators) {
        this.runManager = runManager;
        this.runGenerators = specGenerators;
    }

    /**
     * Returns a list of active runs.
     */
    @GET
    public Response getRuns() {
        return ResponseUtils.jsonOkResponse(new JSONArray(runManager.getRunNames()));
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
        // Select the matching generator.
        final RunGenerator runGenerator;
        if (StringUtils.isEmpty(type)) {
            if (runGenerators.size() == 1) {
                // If no type is specified and we only support one type of run, then use that type.
                runGenerator = runGenerators.values().iterator().next();
            } else {
                // More than one type available. Type parameter is required.
                return ResponseUtils.plainResponse(
                        String.format("Missing 'type' parameter. Must be one of: %s", runGenerators.keySet()),
                        Response.Status.BAD_REQUEST);
            }
        } else {
            runGenerator = runGenerators.get(type);
            if (runGenerator == null) {
                // Bad type.
                return ResponseUtils.plainResponse(
                        String.format("Invalid 'type' value '%s'. Must be one of: %s", type, runGenerators.keySet()),
                        Response.Status.BAD_REQUEST);
            }
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
        } catch (Exception e) {
            logger.error("Failed to generate service", e);
            return ResponseUtils.plainResponse(
                    String.format("Failed to generate service from provided payload: %s", e.getMessage()),
                    Response.Status.BAD_REQUEST);
        }

        // TODO(nickbp): At this point, store the payload so that it can be re-added after a restart

        try {
            runManager.putRun(run);
            JSONObject obj = new JSONObject();
            obj.put("name", run.getName());
            return ResponseUtils.jsonOkResponse(obj);
        } catch (Exception e) {
            // TODO(nickbp): Remove the run from the above storage here
            logger.error("Failed to add run", e);
            return ResponseUtils.plainResponse(
                    String.format("Failed to add run: %s", e.getMessage()), Response.Status.BAD_REQUEST);
        }
    }
}
