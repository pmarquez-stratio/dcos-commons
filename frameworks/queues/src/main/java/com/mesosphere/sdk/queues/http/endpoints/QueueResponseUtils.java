package com.mesosphere.sdk.queues.http.endpoints;

import javax.ws.rs.core.Response;

import com.mesosphere.sdk.http.ResponseUtils;

/**
 * Utilities for common responses to Runs endpoints.
 */
public class QueueResponseUtils {

    private QueueResponseUtils() {
        // do not instantiate
    }

    /**
     * Returns a "404 Run [runName] not found" response.
     */
    public static Response runNotFoundResponse(String runName) {
        return ResponseUtils.notFoundResponse("Run " + runName);
    }
}
