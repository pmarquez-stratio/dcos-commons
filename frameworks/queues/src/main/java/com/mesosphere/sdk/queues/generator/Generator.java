package com.mesosphere.sdk.queues.generator;

import com.mesosphere.sdk.scheduler.AbstractScheduler;

/**
 * Converts HTTP payloads to constructed services.
 */
public interface Generator {

    /**
     * Given the provided raw data sent in an HTTP request, constructs and returns a service object which will execute
     * the workload described in the request.
     *
     * @param data the payload data, which is no larger than {@link #getPayloadSizeLimitBytes()}
     * @return a constructed service object which will execute the run
     * @throws Exception if the content was invalid
     */
    AbstractScheduler generate(byte[] data) throws Exception;

    /**
     * Returns the maximum number of bytes that a payload of this type should fit within, or <=0 for no limit
     * (not recommended!).
     */
    int getMaxDataSizeBytes();
}
