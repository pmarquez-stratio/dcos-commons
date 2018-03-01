package com.mesosphere.sdk.queues.http.endpoints;

import com.mesosphere.sdk.http.ResponseUtils;
import com.mesosphere.sdk.queues.http.types.JobInfoProvider;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

/**
 * A read-only API for listing available jobs.
 */
@Path("/v1/jobs")
public class JobsResource {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final JobInfoProvider jobInfoProvider;

    public JobsResource(JobInfoProvider jobInfoProvider) {
        this.jobInfoProvider = jobInfoProvider;
    }

    /**
     * Returns a list of active jobs.
     */
    @GET
    public Response getJobs() {
        return ResponseUtils.jsonOkResponse(new JSONArray(jobInfoProvider.getJobs()));
    }

    @POST
    public Response addJob(String jobYaml) {
        logger.info("Job YAML: {}", jobYaml);
        return ResponseUtils.plainOkResponse("Accepted job.");
    }
}
