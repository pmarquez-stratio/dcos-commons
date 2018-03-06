package com.mesosphere.sdk.queues.http.endpoints;

import com.mesosphere.sdk.http.queries.PlansQueries;
import com.mesosphere.sdk.http.types.PrettyJsonResource;
import com.mesosphere.sdk.queues.http.types.RunInfoProvider;
import com.mesosphere.sdk.scheduler.plan.PlanCoordinator;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Map;
import java.util.Optional;

/**
 * API for management of Plan(s).
 */
@Path("/v1/runs")
public class RunsPlansResource extends PrettyJsonResource {

    private final RunInfoProvider runInfoProvider;

    /**
     * Creates a new instance which allows access to plans for runs in the provider.
     */
    public RunsPlansResource(RunInfoProvider runInfoProvider) {
        this.runInfoProvider = runInfoProvider;
    }

    /**
     * @see PlansQueries
     */
    @Path("{runName}/plans")
    @GET
    public Response list(@PathParam("runName") String runName) {
        Optional<PlanCoordinator> planCoordinator = runInfoProvider.getPlanCoordinator(runName);
        if (!planCoordinator.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return PlansQueries.list(planCoordinator.get().getPlanManagers());
    }

    /**
     * @see PlansQueries
     */
    @GET
    @Path("{runName}/plans/{planName}")
    public Response get(@PathParam("runName") String runName, @PathParam("planName") String planName) {
        Optional<PlanCoordinator> planCoordinator = runInfoProvider.getPlanCoordinator(runName);
        if (!planCoordinator.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return PlansQueries.get(planCoordinator.get().getPlanManagers(), planName);
    }

    /**
     * @see PlansQueries
     */
    @POST
    @Path("{runName}/plans/{planName}/start")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response start(
            @PathParam("runName") String runName,
            @PathParam("planName") String planName,
            Map<String, String> parameters) {
        Optional<PlanCoordinator> planCoordinator = runInfoProvider.getPlanCoordinator(runName);
        if (!planCoordinator.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return PlansQueries.start(planCoordinator.get().getPlanManagers(), planName, parameters);
    }

    /**
     * @see PlansQueries
     */
    @POST
    @Path("{runName}/plans/{planName}/stop")
    public Response stop(@PathParam("runName") String runName, @PathParam("planName") String planName) {
        Optional<PlanCoordinator> planCoordinator = runInfoProvider.getPlanCoordinator(runName);
        if (!planCoordinator.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return PlansQueries.stop(planCoordinator.get().getPlanManagers(), planName);
    }

    /**
     * @see PlansQueries
     */
    @POST
    @Path("{runName}/plans/{planName}/continue")
    public Response continuePlan(
            @PathParam("runName") String runName,
            @PathParam("planName") String planName,
            @QueryParam("phase") String phase) {
        Optional<PlanCoordinator> planCoordinator = runInfoProvider.getPlanCoordinator(runName);
        if (!planCoordinator.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return PlansQueries.continuePlan(planCoordinator.get().getPlanManagers(), planName, phase);
    }

    /**
     * @see PlansQueries
     */
    @POST
    @Path("{runName}/plans/{planName}/interrupt")
    public Response interrupt(
            @PathParam("runName") String runName,
            @PathParam("planName") String planName,
            @QueryParam("phase") String phase) {
        Optional<PlanCoordinator> planCoordinator = runInfoProvider.getPlanCoordinator(runName);
        if (!planCoordinator.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return PlansQueries.interrupt(planCoordinator.get().getPlanManagers(), planName, phase);
    }

    /**
     * @see PlansQueries
     */
    @POST
    @Path("{runName}/plans/{planName}/forceComplete")
    public Response forceComplete(
            @PathParam("runName") String runName,
            @PathParam("planName") String planName,
            @QueryParam("phase") String phase,
            @QueryParam("step") String step) {
        Optional<PlanCoordinator> planCoordinator = runInfoProvider.getPlanCoordinator(runName);
        if (!planCoordinator.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return PlansQueries.forceComplete(planCoordinator.get().getPlanManagers(), planName, phase, step);
    }

    /**
     * @see PlansQueries
     */
    @POST
    @Path("{runName}/plans/{planName}/restart")
    public Response restart(
            @PathParam("runName") String runName,
            @PathParam("planName") String planName,
            @QueryParam("phase") String phase,
            @QueryParam("step") String step) {
        Optional<PlanCoordinator> planCoordinator = runInfoProvider.getPlanCoordinator(runName);
        if (!planCoordinator.isPresent()) {
            return RunResponseUtils.runNotFoundResponse(runName);
        }
        return PlansQueries.restart(planCoordinator.get().getPlanManagers(), planName, phase, step);
    }
}
