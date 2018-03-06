package com.mesosphere.sdk.queues.http.endpoints;

import com.mesosphere.sdk.http.ResponseUtils;
import com.mesosphere.sdk.queues.http.types.RunInfoProvider;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

import org.json.JSONArray;

/**
 * A read-only API for listing available runs.
 */
@Path("/v1/runs")
public class RunsResource {

    private final RunInfoProvider runInfoProvider;

    public RunsResource(RunInfoProvider runInfoProvider) {
        this.runInfoProvider = runInfoProvider;
    }

    /**
     * Returns a list of active runs.
     */
    @GET
    public Response getRuns() {
        return ResponseUtils.jsonOkResponse(new JSONArray(runInfoProvider.getRuns()));
    }
}
