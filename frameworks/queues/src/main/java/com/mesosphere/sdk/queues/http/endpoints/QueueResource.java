package com.mesosphere.sdk.queues.http.endpoints;

import com.mesosphere.sdk.http.ResponseUtils;
import com.mesosphere.sdk.queues.http.types.QueueInfoProvider;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

import org.json.JSONArray;

/**
 * A read-only API for listing available runs.
 */
@Path("/v1/runs")
public class QueueResource {

    private final QueueInfoProvider runInfoProvider;

    public QueueResource(QueueInfoProvider runInfoProvider) {
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
