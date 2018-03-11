package com.mesosphere.sdk.scheduler.uninstall;

import com.mesosphere.sdk.scheduler.plan.PodInstanceRequirement;
import com.mesosphere.sdk.scheduler.plan.Status;

import java.util.Optional;

/**
 * Step which advertises that the service has been deregistered.
 */
public class DeregisterStep extends UninstallStep {

    public DeregisterStep() {
        super("deregister", Status.PENDING);
    }

    @Override
    public Optional<PodInstanceRequirement> start() {
        if (isPending()) {
            setStatus(Status.PREPARED);
        }

        return getPodInstanceRequirement();
    }

    /**
     * Marks this step complete after the framework has been deregistered.
     * At this point, the overall {@code deploy} plan for uninstall should be complete.
     */
    public void setComplete() {
        setStatus(Status.COMPLETE);
    }
}
