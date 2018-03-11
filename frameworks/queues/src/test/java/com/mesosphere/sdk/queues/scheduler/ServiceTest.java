package com.mesosphere.sdk.queues.scheduler;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import com.mesosphere.sdk.testing.ServiceTestRunner;

public class ServiceTest {

    /**
     * Arbitrary yaml service spec to exercise below.
     */
    private static final String MINIMAL_SPEC =
            "name: SVCNAMEsvc\n"
            + "scheduler:\n"
            + "  user: nobody\n"
            + "pods:\n"
            + "  SVCNAMEpod:\n"
            + "    count: 2\n"
            + "    tasks:\n"
            + "      node:\n"
            + "        goal: RUNNING\n"
            + "        cmd: \"echo SVCNAME >> output && sleep 1000\"\n"
            + "        cpus: 0.1\n"
            + "        memory: 252\n";

    @Test
    public void testSpec() throws Exception {
        // Just validate universe templating:
        File templateFile = File.createTempFile("template", ".yml");
        FileUtils.write(templateFile, MINIMAL_SPEC);
        try {
            new ServiceTestRunner(templateFile).run();
        } finally {
            Assert.assertTrue(templateFile.delete());
        }
    }
}
