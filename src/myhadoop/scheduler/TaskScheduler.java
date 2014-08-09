package myhadoop.scheduler;

import java.io.IOException;
import java.util.List;

import myhadoop.jobtracker.JobInProgress;
import myhadoop.tasktracker.Task;
import myhadoop.tasktracker.TaskTrackerStatus;
import myhadoop.utility.Configuration;

/**
 * Base class of all task scheduler, it defines 
 * the necessary variables and functions a job
 * scheduler should use. 
 *
 */
public abstract class TaskScheduler {
	protected TaskTrackerManager taskTrackerManager;

	protected TaskSchedulerContext context;

	public TaskScheduler(Configuration conf) {
		this.context = new TaskSchedulerContext(conf);
	}

	public synchronized void setTaskTrackerManager(TaskTrackerManager taskTrackerManager) {
		this.taskTrackerManager = taskTrackerManager;
	}

	public TaskTrackerManager getTaskTrackerManager() {
		return taskTrackerManager;
	}

	/**
	 * Lifecycle method to allow the scheduler to start any work in separate
	 * threads.
	 * 
	 * @throws IOException
	 */
	public abstract void start() throws IOException;

	/**
	 * Lifecycle method to allow the scheduler to stop any work it is doing.
	 * 
	 * @throws IOException
	 */
	public abstract void terminate() throws IOException;

	/**
	 * Returns the tasks we'd like the TaskTracker to execute right now.
	 * 
	 * @param taskTracker
	 *            The TaskTracker for which we're looking for tasks.
	 * @return A list of tasks to run on that TaskTracker, possibly empty.
	 */
	public abstract List<Task> assignTasks(TaskTrackerStatus taskTracker) throws IOException;

	/**
	 * Returns a collection of jobs in an order which is specific to the
	 * particular scheduler.
	 * 
	 * @param queueName
	 * @return
	 */
	public abstract List<JobInProgress> getJobs(String queueName);

	public abstract void addJob(JobInProgress jobInProgress);

	public abstract void removeJob(JobInProgress jobInProgress);

	public abstract void updateJob(JobInProgress jobInProgress, String jobEvent);
}
