package com.mesosphere.sdk.queues.http.endpoints;

import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.junit.Test;

public class JobsArtifactResourceTest {

    @Test
    public void testGetQueuesTemplateUrl() {
        UUID uuid = UUID.randomUUID();
        assertEquals(
                "http://api.fwk-name.marathon.l4lb.thisdcos.directory/v1/jobs/job-name/artifacts/template/"
                        + uuid.toString() + "/some-pod/some-task/some-config",
                JobsArtifactResource.getUrlFactory("fwk-name", "job-name").get(uuid, "some-pod", "some-task", "some-config"));
        assertEquals(
                // TODO(nickbp): figure something out for slashes in job names, or just disallow them...
                "http://api.pathtofwk-name.marathon.l4lb.thisdcos.directory/v1/jobs//path/to/job-name/artifacts/template/"
                        + uuid.toString() + "/some-pod/some-task/some-config",
                JobsArtifactResource.getUrlFactory("/path/to/fwk-name", "/path/to/job-name").get(uuid, "some-pod", "some-task", "some-config"));
    }
}
