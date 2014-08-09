package myhadoop.scheduler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import myhadoop.jobtracker.JobInProgress;
import myhadoop.tasktracker.Task;
import myhadoop.tasktracker.TaskTrackerStatus;
import myhadoop.utility.Configuration;
import myhadoop.utility.Log;

/**
 * This class is a implementation of TaskScheduler. It manages two listeners.
 * The queue listener is responsible for managing the FIFO queue of the job on a
 * job tracker. The initialization listener is responsible for asyn
 * initializing the job on a job tracker.
 * 
 */
public class FIFOTaskScheduler extends TaskScheduler {
	protected JobInitializationListener jobInitializationListener;
	protected JobQueueListener jobQueueListener;

	public FIFOTaskScheduler(Configuration conf) {
		super(conf);
	}

	@Override
	public synchronized void start() throws IOException {
		Log.write("Start FIFO task scheduler");
		this.jobInitializationListener = new JobInitializationListener(context.getConf());
		this.jobInitializationListener.setTaskTrackerManager(this.getTaskTrackerManager());
		this.jobInitializationListener.start();
		this.jobQueueListener = new JobQueueListener();
	}

	@Override
	public synchronized void terminate() throws IOException {
		Log.write("Terminate FIFO task scheduler");
		this.jobInitializationListener.terminate();
	}

	/**
	 * This function is used to assign tasks to a task tracker. It is called by
	 * the job tracker when it receives a heartbeat from the task tracker.
	 */
	@Override
	public List<Task> assignTasks(TaskTrackerStatus status) throws IOException {
		ClusterStatus clusterStatus = taskTrackerManager.getClusterStatus();
		List<JobInProgress> jobQueue = jobQueueListener.getJobQueue();
		List<Task> assignedTaskList = new ArrayList<Task>();

		// Compute remaining map and reduce tasks in the pool
		int remaingingMapTaskNum = 0;
		int remaingingReduceTaskNum = 0;
		synchronized (jobQueue) {
			for (JobInProgress jobInProgress : jobQueue) {
				if (jobInProgress.getState().isRunning()) {
					remaingingMapTaskNum += jobInProgress.getMapTaskNum() - jobInProgress.getFinishedMapTaskNum();
					if (jobInProgress.canScheduleReduceTask()) {
						remaingingReduceTaskNum += jobInProgress.getReduceTaskNum()
								- jobInProgress.getFinishedReduceTaskNum();
					}
				}
			}
		}

		// Compute the load factor for the maps and reduces
		double mapLoadFactor = 0.0;
		if (clusterStatus.getTotalMapSlots() > 0) {
			mapLoadFactor = (double) remaingingMapTaskNum / clusterStatus.getTotalMapSlots();
			mapLoadFactor = Math.min(1, mapLoadFactor);
		}
		double reduceLoadFactor = 0.0;
		if (clusterStatus.getTotalReduceSlots() > 0) {
			reduceLoadFactor = (double) remaingingReduceTaskNum / clusterStatus.getTotalReduceSlots();
			reduceLoadFactor = Math.min(1, reduceLoadFactor);
		}

		// Compute the number of map/reduce tasks should be assigned to this
		// task tracker
		int assignableMapTaskNum = (int) ((mapLoadFactor * status.getMapTaskSlotNum()) - status.countMapTasks());
		int assignableReduceTaskNum = (int) ((reduceLoadFactor * status.getReduceTaskSlotNum()) - status
				.countReduceTasks());

		// Assign map tasks
		int assignedMapTaskNum = 0;
		for (int i = 0; i <= assignableMapTaskNum - 1; i++) {
			synchronized (jobQueue) {
				for (JobInProgress jobInProgress : jobQueue) {
					if (jobInProgress.getState().isRunning()) {
						Task task = jobInProgress.obtainNewMapTask(status);

						if (task != null) {
							assignedTaskList.add(task);
							assignedMapTaskNum += 1;
							break;
						}
					}
				}
			}
		}
		Log.write("Assign " + assignedMapTaskNum + " map tasks to " + status.getTaskTrackerID());

		// Assign reduce tasks
		int assignedReduceTaskNum = 0;
		for (int i = 0; i <= assignableReduceTaskNum - 1; i++) {
			synchronized (jobQueue) {
				for (JobInProgress jobInProgress : jobQueue) {
					if (jobInProgress.getState().isRunning()) {
						Task task = jobInProgress.obtainNewReduceTask(status);

						if (task != null) {
							assignedTaskList.add(task);
							assignedReduceTaskNum += 1;
							break;
						}
					}
				}
			}
		}
		Log.write("Assign " + assignedReduceTaskNum + " reduce tasks to " + status.getTaskTrackerID());
		return assignedTaskList;
	}

	@Override
	public List<JobInProgress> getJobs(String queueName) {
		return null;
	}

	@Override
	public synchronized void addJob(JobInProgress jobInProgress) {
		jobInitializationListener.jobAdded(jobInProgress);
		jobQueueListener.jobAdded(jobInProgress);
	}

	@Override
	public synchronized void removeJob(JobInProgress jobInProgress) {
		jobInitializationListener.jobRemoved(jobInProgress);
		jobQueueListener.jobAdded(jobInProgress);
	}

	@Override
	public void updateJob(JobInProgress jobInProgress, String jobEvent) {
		jobInitializationListener.jobUpdated(jobInProgress, jobEvent);
		jobQueueListener.jobUpdated(jobInProgress, jobEvent);
	}
}
