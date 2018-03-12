package com.mesosphere.sdk.queues.generator;

import com.mesosphere.sdk.scheduler.AbstractScheduler;

/**
 * Default implementation of {@link Generator} with support for run types in the default queue service.
 */
public class SparkGenerator implements Generator {

    // 64k ought to be good enough for anybody.
    private static final int PAYLOAD_SIZE_LIMIT = 65536;

    @Override
    public AbstractScheduler generate(byte[] data) {
        // TODO(nickbp): Convert to spark AbstractScheduler
        return null;
    }

    @Override
    public int getMaxDataSizeBytes() {
        return PAYLOAD_SIZE_LIMIT;
    }
}
