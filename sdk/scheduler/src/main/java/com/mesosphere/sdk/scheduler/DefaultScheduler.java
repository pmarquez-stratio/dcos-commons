package com.mesosphere.sdk.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.TextFormat;
import com.mesosphere.sdk.dcos.Capabilities;
import com.mesosphere.sdk.http.endpoints.*;
import com.mesosphere.sdk.http.types.EndpointProducer;
import com.mesosphere.sdk.http.types.StringPropertyDeserializer;
import com.mesosphere.sdk.offer.*;
import com.mesosphere.sdk.offer.evaluate.OfferEvaluator;
import com.mesosphere.sdk.offer.history.OfferOutcomeTracker;
import com.mesosphere.sdk.scheduler.decommission.DecommissionPlanFactory;
import com.mesosphere.sdk.scheduler.decommission.DecommissionRecorder;
import com.mesosphere.sdk.scheduler.plan.*;
import com.mesosphere.sdk.scheduler.recovery.FailureUtils;
import com.mesosphere.sdk.specification.ServiceSpec;
import com.mesosphere.sdk.state.*;
import com.mesosphere.sdk.storage.Persister;
import com.mesosphere.sdk.storage.PersisterException;
import org.apache.mesos.Protos;
import org.slf4j.Logger;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This scheduler when provided with a ServiceSpec will deploy the service and recover from encountered faults
 * when possible.  Changes to the ServiceSpec will result in rolling configuration updates, or the creation of
 * new Tasks where applicable.
 */
public class DefaultScheduler extends ServiceScheduler {

    private final String serviceName;
    private final Logger logger;
    private final ConfigStore<ServiceSpec> configStore;
    private final PlanCoordinator planCoordinator;
    private final Collection<Object> customResources;
    private final Map<String, EndpointProducer> customEndpointProducers;
    private final OperationRecorder launchRecorder;
    private final Optional<OperationRecorder> decommissionRecorder;
    private final OfferOutcomeTracker offerOutcomeTracker;
    private final PlanScheduler planScheduler;

    /**
     * Creates a new {@link SchedulerBuilder} based on the provided {@link ServiceSpec} describing the service,
     * including details such as the service name, the pods/tasks to be deployed, and the plans describing how the
     * deployment should be organized.
     */
    public static SchedulerBuilder newBuilder(
            ServiceSpec serviceSpec, SchedulerConfig schedulerConfig) throws PersisterException {
        return new SchedulerBuilder(serviceSpec, schedulerConfig);
    }

    /**
     * Creates a new {@link SchedulerBuilder} based on the provided {@link ServiceSpec} describing the service,
     * including details such as the service name, the pods/tasks to be deployed, and the plans describing how the
     * deployment should be organized.
     */
    @VisibleForTesting
    public static SchedulerBuilder newBuilder(
            ServiceSpec serviceSpec, SchedulerConfig schedulerConfig, Persister persister) throws PersisterException {
        return new SchedulerBuilder(serviceSpec, schedulerConfig, persister);
    }

    /**
     * Creates a new DefaultScheduler. See information about parameters in {@link SchedulerBuilder}.
     */
    protected DefaultScheduler(
            ServiceSpec serviceSpec,
            Optional<FrameworkConfig> multiServiceFrameworkConfig,
            SchedulerConfig schedulerConfig,
            Collection<Object> customResources,
            PlanCoordinator planCoordinator,
            Optional<PlanCustomizer> planCustomizer,
            FrameworkStore frameworkStore,
            StateStore stateStore,
            ConfigStore<ServiceSpec> configStore,
            Map<String, EndpointProducer> customEndpointProducers) throws ConfigStoreException {
        super(serviceSpec.getName(), frameworkStore, stateStore, schedulerConfig, planCustomizer);
        this.serviceName = serviceSpec.getName();
        this.logger = LoggingUtils.getLogger(getClass(), serviceName);
        this.configStore = configStore;
        this.planCoordinator = planCoordinator;
        this.customResources = customResources;
        this.customEndpointProducers = customEndpointProducers;

        this.launchRecorder = new PersistentLaunchRecorder(stateStore, serviceSpec);
        Optional<DecommissionPlanManager> decommissionManager = getDecommissionManager(planCoordinator);
        if (decommissionManager.isPresent()) {
            Collection<Step> steps = decommissionManager.get().getPlan().getChildren().stream()
                    .flatMap(phase -> phase.getChildren().stream())
                    .collect(Collectors.toList());
            this.decommissionRecorder = Optional.of(new DecommissionRecorder(serviceSpec.getName(), stateStore, steps));
        } else {
            this.decommissionRecorder = Optional.empty();
        }

        this.offerOutcomeTracker = new OfferOutcomeTracker();
        this.planScheduler = new PlanScheduler(
                serviceSpec.getName(),
                new OfferEvaluator(
                        frameworkStore,
                        stateStore,
                        offerOutcomeTracker,
                        serviceSpec.getName(),
                        multiServiceFrameworkConfig,
                        configStore.getTargetConfig(),
                        schedulerConfig,
                        Capabilities.getInstance().supportsDefaultExecutor()),
                stateStore);
    }

    @Override
    public Collection<Object> getHTTPEndpoints() {
        Collection<Object> resources = new ArrayList<>();
        resources.addAll(customResources);
        resources.add(new ArtifactResource(configStore));
        resources.add(new ConfigResource<>(configStore));
        EndpointsResource endpointsResource = new EndpointsResource(stateStore, serviceName);
        for (Map.Entry<String, EndpointProducer> entry : customEndpointProducers.entrySet()) {
            endpointsResource.setCustomEndpoint(entry.getKey(), entry.getValue());
        }
        resources.add(endpointsResource);
        PlansResource plansResource = new PlansResource(planCoordinator);
        resources.add(plansResource);
        resources.add(new DeprecatedPlanResource(plansResource));
        resources.add(new HealthResource(planCoordinator));
        resources.add(new PodResource(stateStore, configStore, serviceName));
        resources.add(new StateResource(frameworkStore, stateStore, new StringPropertyDeserializer()));
        resources.add(new OfferOutcomeResource(offerOutcomeTracker));
        return resources;
    }

    @Override
    public PlanCoordinator getPlanCoordinator() {
        return planCoordinator;
    }

    @Override
    public ConfigStore<ServiceSpec> getConfigStore() {
        return configStore;
    }

    @Override
    protected void registeredWithMesos() {
        Set<String> activeTasks = PlanUtils.getLaunchableTasks(getPlans());

        Optional<DecommissionPlanManager> decomissionManager = getDecommissionManager(getPlanCoordinator());
        if (decomissionManager.isPresent()) {
            Collection<String> decommissionedTasks = decomissionManager.get().getTasksToDecommission().stream()
                    .map(taskInfo -> taskInfo.getName())
                    .collect(Collectors.toList());
            activeTasks.addAll(decommissionedTasks);
        }

        killUnneededTasks(stateStore, activeTasks);
    }

    private static Optional<DecommissionPlanManager> getDecommissionManager(PlanCoordinator planCoordinator) {
        return planCoordinator.getPlanManagers().stream()
                .filter(planManager -> planManager.getPlan().isDecommissionPlan())
                .map(planManager -> (DecommissionPlanManager) planManager)
                .findFirst();
    }

    private static void killUnneededTasks(StateStore stateStore, Set<String> taskToDeployNames) {
        Set<Protos.TaskInfo> unneededTaskInfos = stateStore.fetchTasks().stream()
                .filter(taskInfo -> !taskToDeployNames.contains(taskInfo.getName()))
                .collect(Collectors.toSet());

        Set<Protos.TaskID> taskIdsToKill = unneededTaskInfos.stream()
                .map(taskInfo -> taskInfo.getTaskId())
                .collect(Collectors.toSet());

        // Clear the TaskIDs from the TaskInfos so we drop all future TaskStatus Messages
        Set<Protos.TaskInfo> cleanedTaskInfos = unneededTaskInfos.stream()
                .map(taskInfo -> taskInfo.toBuilder())
                .map(builder -> builder.setTaskId(Protos.TaskID.newBuilder().setValue("")).build())
                .collect(Collectors.toSet());

        // Remove both TaskInfo and TaskStatus, then store the cleaned TaskInfo one at a time to limit damage in the
        // event of an untimely scheduler crash
        for (Protos.TaskInfo taskInfo : cleanedTaskInfos) {
            stateStore.clearTask(taskInfo.getName());
            stateStore.storeTasks(Arrays.asList(taskInfo));
        }

        taskIdsToKill.forEach(taskID -> TaskKiller.killTask(taskID));

        for (Protos.TaskInfo taskInfo : stateStore.fetchTasks()) {
            GoalStateOverride.Status overrideStatus = stateStore.fetchGoalOverrideStatus(taskInfo.getName());
            if (overrideStatus.progress == GoalStateOverride.Progress.PENDING) {
                // Enabling or disabling an override was triggered, but the task kill wasn't processed so that the
                // change in override could take effect. Kill the task so that it can enter (or exit) the override. The
                // override status will then be marked IN_PROGRESS once we have received the terminal TaskStatus.
                TaskKiller.killTask(taskInfo.getTaskId());
            }
        }
    }

    @Override
    protected List<OfferRecommendation> processOffers(List<Protos.Offer> offers, Collection<Step> steps) {
        // See which offers are useful to the plans, then omit the ones that shouldn't be launched.
        List<OfferRecommendation> offerRecommendations = getOfferRecommendations(logger, planScheduler, offers, steps);

        logger.info("{} Offer{} processed: {} accepted by {} scheduler: {}",
                offers.size(),
                offers.size() == 1 ? "" : "s",
                offerRecommendations.size(),
                serviceName,
                offerRecommendations.stream()
                        .map(rec -> rec.getOffer().getId().getValue())
                        .collect(Collectors.toList()));

        try {
            launchRecorder.record(offerRecommendations);
            if (decommissionRecorder.isPresent()) {
                decommissionRecorder.get().record(offerRecommendations);
            }
        } catch (Exception ex) {
            // TODO(nickbp): This doesn't undo prior recorded operations, so things could be left in a bad state.
            logger.error("Failed to record offer operations", ex);
        }

        return offerRecommendations;
    }

    /**
     * Returns the operations to be performed against the provided steps, according to the provided
     * {@link PlanScheduler}. Any {@link LaunchOfferRecommendation}s with {@code !shouldLaunch()} will be omitted from
     * the returned list.
     */
    @VisibleForTesting
    static List<OfferRecommendation> getOfferRecommendations(
            Logger logger, PlanScheduler planScheduler, List<Protos.Offer> offers, Collection<Step> steps) {
        List<OfferRecommendation> filteredOfferRecommendations = new ArrayList<>();
        for (OfferRecommendation offerRecommendation : planScheduler.resourceOffers(offers, steps)) {
            if (offerRecommendation instanceof LaunchOfferRecommendation &&
                    !((LaunchOfferRecommendation) offerRecommendation).shouldLaunch()) {
                logger.info("Skipping launch of transient Operation: {}",
                        TextFormat.shortDebugString(offerRecommendation.getOperation()));
            } else {
                filteredOfferRecommendations.add(offerRecommendation);
            }
        }
        return filteredOfferRecommendations;
    }

    @Override
    public void cleaned(Collection<OfferRecommendation> recommendations) {
        try {
            if (decommissionRecorder.isPresent()) {
                decommissionRecorder.get().record(recommendations);
            }
        } catch (Exception ex) {
            // TODO(nickbp): This doesn't undo prior recorded operations, so things could be left in a bad state.
            logger.error("Failed to record cleanup operations", ex);
        }
    }

    @Override
    protected void processStatusUpdate(Protos.TaskStatus status) throws Exception {
        // Store status, then pass status to PlanManager => Plan => Steps
        String taskName = StateStoreUtils.getTaskName(stateStore, status);

        // StateStore updates:
        // - TaskStatus
        // - Override status (if applicable)
        stateStore.storeStatus(taskName, status);

        // Notify plans of status update:
        planCoordinator.getPlanManagers().forEach(planManager -> planManager.update(status));

        // If the TaskStatus contains an IP Address, store it as a property in the StateStore.
        // We expect the TaskStatus to contain an IP address in both Host or CNI networking.
        // Currently, we are always _missing_ the IP Address on TASK_LOST. We always expect it on TASK_RUNNINGs
        if (status.hasContainerStatus() &&
                status.getContainerStatus().getNetworkInfosCount() > 0 &&
                status.getContainerStatus().getNetworkInfosList().stream()
                        .anyMatch(networkInfo -> networkInfo.getIpAddressesCount() > 0)) {
            // Map the TaskStatus to a TaskInfo. The map will throw a StateStoreException if no such TaskInfo exists.
            try {
                StateStoreUtils.storeTaskStatusAsProperty(stateStore, taskName, status);
            } catch (StateStoreException e) {
                logger.warn("Unable to store network info for status update: " + status, e);
            }
        }
    }

    @Override
    public Collection<Protos.Resource> getExpectedResources() {
        return stateStore.fetchTasks().stream()
                // The task's resources should be unreserved if:
                // - the task is marked as permanently failed, or
                // - the task is in the process of being decommissioned
                .filter(taskInfo ->
                        !FailureUtils.isPermanentlyFailed(taskInfo) &&
                        !stateStore.fetchGoalOverrideStatus(taskInfo.getName())
                                .equals(DecommissionPlanFactory.DECOMMISSIONING_STATUS))
                .map(ResourceUtils::getAllResources)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }
}
