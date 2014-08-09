package myhadoop.scheduler;

import myhadoop.jobtracker.JobInProgress;

/**
 * This interface is an abstract of the Jobtracker. It defines some useful
 * functions for TaskSchedulers to use.
 */
public interface TaskTrackerManager {

	public void initJob(JobInProgress jobInProgress);

	public void failJob(JobInProgress jobInProgress);
	
	public ClusterStatus getClusterStatus();
}
