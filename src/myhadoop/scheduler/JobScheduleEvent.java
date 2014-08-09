package myhadoop.scheduler;

/**
 * Job scheduler event passed to a job listener to notify the listener the
 * status of a job has changed.
 * 
 */
public class JobScheduleEvent {
	public static String JOB_FINISHED = "JOB_FINISHED";
	public static String JOB_FAILED = "JOB_FAILED";
}
