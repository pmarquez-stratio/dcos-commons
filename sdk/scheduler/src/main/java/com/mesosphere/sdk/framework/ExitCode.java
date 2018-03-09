package com.mesosphere.sdk.framework;

/**
 * This enum provides exit codes for Schedulers.
 */
public class ExitCode {

    // Commented items are no longer used, but their numbers may be repurposed later
    public static final ExitCode SUCCESS = new ExitCode(0);
    public static final ExitCode INITIALIZATION_FAILURE = new ExitCode(1);
    public static final ExitCode REGISTRATION_FAILURE = new ExitCode(2);
    //public static final SchedulerErrorCode RE_REGISTRATION = new SchedulerErrorCode(3);
    //public static final SchedulerErrorCode OFFER_RESCINDED = new SchedulerErrorCode(4);
    public static final ExitCode DISCONNECTED = new ExitCode(5);
    public static final ExitCode ERROR = new ExitCode(6);
    //public static final SchedulerErrorCode PLAN_CREATE_FAILURE = new SchedulerErrorCode(7);
    public static final ExitCode LOCK_UNAVAILABLE = new ExitCode(8);
    public static final ExitCode API_SERVER_ERROR = new ExitCode(9);
    //public static final SchedulerErrorCode SCHEDULER_BUILD_FAILED = new SchedulerErrorCode(10);
    public static final ExitCode SCHEDULER_ALREADY_UNINSTALLING = new ExitCode(11);
    //public static final SchedulerErrorCode SCHEDULER_INITIALIZATION_FAILURE = new SchedulerErrorCode(12);
    public static final ExitCode DRIVER_EXITED = new ExitCode(13);

    private final int value;

    private ExitCode(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
