package com.mesosphere.sdk.queues.http.endpoints;

import com.mesosphere.sdk.http.ResponseUtils;
import com.mesosphere.sdk.queues.http.types.JobInfoProvider;
import com.mesosphere.sdk.specification.yaml.RawServiceSpec;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.time.LocalTime;

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
        String localTime = LocalTime.now().toString();
        String renderName = String.format("job_%s", localTime);
        logger.info("Rendering {} with input {}", renderName, jobYaml);

        try {
            RawServiceSpec rawServiceSpec = RawServiceSpec.newBuilder(renderName, jobYaml).build();
            return ResponseUtils.plainOkResponse(String.format("Accepted job: %s", rawServiceSpec.getName()));
        } catch (Exception e) {
            String errMsg = "Failed to render service spec";
            logger.error("{}: {}", errMsg, e);
            return ResponseUtils.plainResponse(
                    String.format("%s: %s", errMsg, e),
                    Response.Status.BAD_REQUEST);
        }
    }
}
