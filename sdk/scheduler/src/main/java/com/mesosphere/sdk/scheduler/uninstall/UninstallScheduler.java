package com.mesosphere.sdk.scheduler.uninstall;

import com.google.common.annotations.VisibleForTesting;
import com.mesosphere.sdk.config.SerializationUtils;
import com.mesosphere.sdk.dcos.clients.SecretsClient;
import com.mesosphere.sdk.http.endpoints.DeprecatedPlanResource;
import com.mesosphere.sdk.http.endpoints.HealthResource;
import com.mesosphere.sdk.http.endpoints.PlansResource;
import com.mesosphere.sdk.http.types.PlanInfo;
import com.mesosphere.sdk.offer.*;
import com.mesosphere.sdk.scheduler.ServiceScheduler;
import com.mesosphere.sdk.scheduler.OfferResources;
import com.mesosphere.sdk.scheduler.SchedulerConfig;
import com.mesosphere.sdk.scheduler.plan.*;
import com.mesosphere.sdk.specification.ServiceSpec;
import com.mesosphere.sdk.state.ConfigStore;
import com.mesosphere.sdk.state.FrameworkStore;
import com.mesosphere.sdk.state.StateStore;
import com.mesosphere.sdk.state.StateStoreUtils;
import org.apache.mesos.Protos;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This scheduler uninstalls a service and releases all of its resources.
 */
public class UninstallScheduler extends ServiceScheduler {
    /**
     * Empty complete deploy plan to be used if the scheduler was launched in a finished state.
     */
    @VisibleForTesting
    static final Plan EMPTY_DEPLOY_PLAN = new DefaultPlan(Constants.DEPLOY_PLAN_NAME, Collections.emptyList());

    private final Logger logger;
    private final ConfigStore<ServiceSpec> configStore;
    private final UninstallRecorder recorder;

    private PlanManager uninstallPlanManager;
    private Collection<Object> resources = Collections.emptyList();

    /**
     * Creates a new {@link UninstallScheduler} based on the provided API port and initialization timeout, and a
     * {@link StateStore}. The {@link UninstallScheduler} builds an uninstall {@link Plan} which will clean up
     * the service's reservations, TLS artifacts, zookeeper data, and any other artifacts from running the service.
     */
    public UninstallScheduler(
            ServiceSpec serviceSpec,
            FrameworkStore frameworkStore,
            StateStore stateStore,
            ConfigStore<ServiceSpec> configStore,
            SchedulerConfig schedulerConfig,
            Optional<PlanCustomizer> planCustomizer) {
        this(serviceSpec, frameworkStore, stateStore, configStore, schedulerConfig, planCustomizer, Optional.empty());
    }

    protected UninstallScheduler(
            ServiceSpec serviceSpec,
            FrameworkStore frameworkStore,
            StateStore stateStore,
            ConfigStore<ServiceSpec> configStore,
            SchedulerConfig schedulerConfig,
            Optional<PlanCustomizer> planCustomizer,
            Optional<SecretsClient> customSecretsClientForTests) {
        super(serviceSpec.getName(), frameworkStore, stateStore, schedulerConfig, planCustomizer);
        this.logger = LoggingUtils.getLogger(getClass(), serviceSpec.getName());
        this.configStore = configStore;

        final Plan deployPlan;
        if (allButStateStoreUninstalled(frameworkStore, stateStore, schedulerConfig)) {
            /**
             * If the state store is empty this scheduler has been deregistered. Therefore it should report itself
             * healthy and provide an empty COMPLETE deploy plan so it may complete its uninstall.
             */
            deployPlan = EMPTY_DEPLOY_PLAN;
        } else {
            deployPlan = new UninstallPlanBuilder(
                    serviceSpec,
                    frameworkStore,
                    stateStore,
                    configStore,
                    schedulerConfig,
                    customSecretsClientForTests)
                    .build();
        }
        this.recorder = new UninstallRecorder(serviceSpec.getName(), stateStore, deployPlan.getChildren().stream()
                .flatMap(phase -> phase.getChildren().stream())
                .filter(step -> step instanceof ResourceCleanupStep)
                .map(step -> (ResourceCleanupStep) step)
                .collect(Collectors.toList()));

        this.uninstallPlanManager = DefaultPlanManager.createProceeding(deployPlan);
        PlansResource plansResource = new PlansResource(Collections.singletonList(uninstallPlanManager));
        this.resources = Arrays.asList(
                plansResource,
                new DeprecatedPlanResource(plansResource),
                new HealthResource(Collections.singletonList(uninstallPlanManager)));
        try {
            logger.info("Uninstall plan set to: {}", SerializationUtils.toJsonString(PlanInfo.forPlan(deployPlan)));
        } catch (IOException e) {
            logger.error("Failed to deserialize uninstall plan.");
        }
    }

    /**
     * Returns whether the process should register with Mesos.
     *
     * This handles the case where there's nothing left to do with Mesos -- the framework has already unregistered.
     */
    public boolean shouldRegisterFramework() {
        return !allButStateStoreUninstalled(frameworkStore, stateStore, schedulerConfig);
    }

    @Override
    public Collection<Object> getHTTPEndpoints() {
        return resources;
    }

    @Override
    public PlanCoordinator getPlanCoordinator() {
        // Return a stub coordinator which only does work against the sole plan manager.
        return new PlanCoordinator() {
            @Override
            public List<Step> getCandidates() {
                return new ArrayList<>(uninstallPlanManager.getCandidates(Collections.emptyList()));
            }

            @Override
            public Collection<PlanManager> getPlanManagers() {
                return Collections.singletonList(uninstallPlanManager);
            }
        };
    }

    @Override
    public ConfigStore<ServiceSpec> getConfigStore() {
        return configStore;
    }

    @Override
    protected void registeredWithMesos() {
        logger.info("Uninstall scheduler registered with Mesos.");
    }

    @Override
    protected List<OfferRecommendation> processOffers(List<Protos.Offer> offers, Collection<Step> steps) {
        // Get candidate steps to be scheduled
        if (!steps.isEmpty()) {
            logger.info("Attempting to process {} candidates from uninstall plan: {}",
                    steps.size(), steps.stream().map(Element::getName).collect(Collectors.toList()));
            steps.forEach(Step::start);
        }

        // No recommendations. Upstream should invoke the cleaner against any unexpected resources in unclaimed offers
        // (including the ones that apply to our service), and then notify us via clean() so that we can record the ones
        // that apply to us.
        return Collections.emptyList();
    }

    /**
     * Returns the resources which are not expected by this service. When uninstalling, all resources are unexpected.
     * The {@link UninstallScheduler} just keeps track of them on its 'checklist' as they are removed.
     */
    @Override
    public UnexpectedResourcesResponse getUnexpectedResources(List<Protos.Offer> unusedOffers) {
        Collection<OfferResources> unexpected = unusedOffers.stream()
                .map(offer -> new OfferResources(offer).addAll(offer.getResourcesList().stream()
                        // Omit unreserved resources:
                        .filter(resource -> ResourceUtils.getReservation(resource).isPresent())
                        .collect(Collectors.toList())))
                .collect(Collectors.toList());
        try {
            recorder.recordResources(unexpected);
            return UnexpectedResourcesResponse.processed(unexpected);
        } catch (Exception e) {
            // Failed to record the upcoming dereservation. Don't return the resources as unexpected until we can record
            // the dereservation.
            logger.error("Failed to record unexpected resources", e);
            return UnexpectedResourcesResponse.failed(Collections.emptyList());
        }
    }

    @Override
    protected void processStatusUpdate(Protos.TaskStatus status) throws Exception {
        stateStore.storeStatus(StateStoreUtils.getTaskName(stateStore, status), status);
    }

    private static boolean allButStateStoreUninstalled(
            FrameworkStore frameworkStore, StateStore stateStore, SchedulerConfig schedulerConfig) {
        // Because we cannot delete the root ZK node (ACLs on the master, see StateStore.clearAllData() for more
        // details) we have to clear everything under it. This results in a race condition, where DefaultService can
        // have register() called after the StateStore already has the uninstall bit wiped.
        //
        // As can be seen in DefaultService.initService(), DefaultService.register() will only be called in uninstall
        // mode if schedulerConfig.isUninstallEnabled() == true. Therefore we can use it as an OR along with
        // StateStoreUtils.isUninstalling().

        // Framework ID is gone, all resources are unreserved/destroyed, but the empty task entries remain to be removed
        return !frameworkStore.fetchFrameworkId().isPresent() &&
                ResourceUtils.getAllResources(stateStore.fetchTasks()).isEmpty();
    }
}
