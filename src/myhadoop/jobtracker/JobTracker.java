package myhadoop.jobtracker;

import java.io.IOException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import myhadoop.fs.FileSystem;
import myhadoop.fs.Path;
import myhadoop.job.JobID;
import myhadoop.jobtracker.JobInProgress.State;
import myhadoop.scheduler.ClusterStatus;
import myhadoop.scheduler.FIFOTaskScheduler;
import myhadoop.scheduler.JobScheduleEvent;
import myhadoop.scheduler.TaskScheduler;
import myhadoop.scheduler.TaskTrackerManager;
import myhadoop.tasktracker.Task;
import myhadoop.tasktracker.TaskAttemptID;
import myhadoop.tasktracker.TaskStatus;
import myhadoop.tasktracker.TaskTrackerID;
import myhadoop.tasktracker.TaskTrackerStatus;
import myhadoop.utility.Configuration;
import myhadoop.utility.Log;

public class JobTracker implements JobTrackerRMI, TaskTrackerManager {
	private JobTrackerContext context;
	private ExpireTaskTrackers expireTaskTrackers;
	private Thread expireTaskTrackersThread;
	// Job ID to job
	private Map<JobID, JobInProgress> jobMap;
	// Task ID to task
	private Map<TaskAttemptID, TaskInProgress> taskAttemptToTaskInProgressMap;
	// Tracker ID to task IDs
	private Map<TaskTrackerID, List<TaskAttemptID>> taskTrackerToTaskAttemptIDsMap;
	// Tracker ID to last sent Heartbeat Response
	private Map<TaskTrackerID, HeartbeatResponse> taskTrackerToHeartbeatResponseMap;
	// Tracker ID to tracker status
	// Only revise this map when a task tracker lost
	private Map<TaskTrackerID, TaskTrackerStatus> taskTrackerToTaskTrackerStatusMap;

	private Integer totalMapSlots = 0;
	private Integer totalReduceSlots = 0;

	private TaskScheduler taskScheduler;

	private int currentJobID = 0;

	private int currentTaskTrackerID = 0;

	private Set<JobID> killedJobSet;

	public JobTracker() {

	}

	/**
	 * RMI function, assign a job ID to a job client.
	 */
	public synchronized int getJobID() throws RemoteException {
		currentJobID += 1;
		return currentJobID;
	}

	/**
	 * RMI function, assign a task tracker ID to a task tracker.
	 */
	public synchronized int getTaskTrackerID() throws RemoteException {
		currentTaskTrackerID += 1;
		return currentTaskTrackerID;
	}

	public JobTrackerContext getContext() {
		return context;
	}

	public TaskScheduler getTaskScheduler() {
		return taskScheduler;
	}

	public void setTaskScheduler(TaskScheduler taskScheduler) {
		this.taskScheduler = taskScheduler;
	}

	/**
	 * RMI function, get a job from a client.
	 * 
	 * @param path
	 *            job configuration path on the fs
	 */
	public synchronized void submitJob(Path path) throws RemoteException {
		JobInProgress jobInProgress = new JobInProgress(context.getFileSystemInstance(), path, this); // TODO changed
		Log.write("Get new job " + jobInProgress.getJobID());

		// Add job to task scheduler
		taskScheduler.addJob(jobInProgress);
		jobMap.put(jobInProgress.getJobID(), jobInProgress);
	}

	public static void main(String[] args){
		JobTracker jobTracker = new JobTracker();
		try {
			jobTracker.initialize();
		} catch (Exception e) {
			e.printStackTrace();
			Log.warn("Initialization error, please try again");
		}
	}

	/**
	 * RMI function, get a hearbeat message from a task tracker
	 */
	public synchronized HeartbeatResponse submitHeartbeat(TaskTrackerStatus taskTrackerStatus,
			boolean canAskForNewTasks, boolean isFirstContact, int responseID) throws RemoteException {
		Log.write("Got heartbeat from:" + taskTrackerStatus.getTaskTrackerID() + " canAskForNewTasks:"
				+ canAskForNewTasks + " isFirstContact:" + isFirstContact + " responseID:" + responseID);
		long now = System.currentTimeMillis();

		HeartbeatResponse prevHeartbeatResponse = taskTrackerToHeartbeatResponseMap.get(taskTrackerStatus
				.getTaskTrackerID());
		if (!isFirstContact) {
			// If this isn't the 'initial contact' from the tasktracker,
			// there is something seriously wrong if the JobTracker has
			// no record of the 'previous heartbeat'; if so, ask the
			// tasktracker to re-initialize itself.
			if (prevHeartbeatResponse == null || prevHeartbeatResponse.getResponseID() != responseID) {
				Log.warn("Serious wrong with " + taskTrackerStatus.getTaskTrackerID()
						+ " Ask the task tracker to restart.");
				// Reinit
				HeartbeatResponse response = new HeartbeatResponse(responseID);
				response.addTaskTrackerAction(new ReinitAction());
				return response;
			}
		}

		int newRespondID = responseID + 1;
		if (!processHeartbeat(taskTrackerStatus, isFirstContact)) {
			Log.warn("Serious wrong with " + taskTrackerStatus.getTaskTrackerID() + " Ask the task tracker to restart.");
			// Reinit
			HeartbeatResponse response = new HeartbeatResponse(responseID);
			response.addTaskTrackerAction(new ReinitAction());
			return response;
		}

		// Init response to task tracker
		HeartbeatResponse heartbeatResponse = new HeartbeatResponse(newRespondID);

		// Get new tasks if possible
		if (canAskForNewTasks) {
			// Assign tasks
			List<Task> taskList = null;
			try {
				taskList = taskScheduler.assignTasks(taskTrackerStatus);
			} catch (IOException e) {
				Log.warn("IOException happens during assign tasks to " + taskTrackerStatus.getTaskTrackerID());
			}

			if (taskList != null) {
				for (Task task : taskList) {
					Log.write("Launch task " + task.getTaskAttemptID() + " on " + taskTrackerStatus.getTaskTrackerID());
					heartbeatResponse.addTaskTrackerAction(new LaunchAction(task));
				}
			}
		}

		// Update heartbeat response map
		taskTrackerToHeartbeatResponseMap.put(taskTrackerStatus.getTaskTrackerID(), heartbeatResponse);
		return heartbeatResponse;
	}

	/**
	 * Process incoming heartbeat messages from the task trackers.
	 */
	private boolean processHeartbeat(TaskTrackerStatus taskTrackerStatus, boolean initialContact) {
		// Updaing task tracker status
		// Must synchronized task tracker related data structure
		synchronized (taskTrackerToTaskTrackerStatusMap) {
			boolean seenBefore = taskTrackerToTaskTrackerStatusMap.containsKey(taskTrackerStatus.getTaskTrackerID());

			if (initialContact) {
				if (seenBefore) {
					// Initial contact but the status has existed, in this
					// situation
					// that means we lost the task tracker.
					lostTaskTracker(taskTrackerStatus);
					return false;
				} else {
					// Normal situation, add new task tracker
					addNewTaskTracker(taskTrackerStatus);
					return true;
				}
			} else {
				if (seenBefore) {
					// Normal case update task tracker status
					updateTaskTrackerStatus(taskTrackerStatus);
					updateTaskStatuses(taskTrackerStatus);
					return true;
				} else {
					return false;
				}
			}
		}
	}

	/**
	 * Update the last recorded status for the given task tracker.
	 * 
	 * @param status
	 *            The new status for the task tracker
	 */
	private synchronized void updateTaskTrackerStatus(TaskTrackerStatus status) {
		if (taskTrackerToTaskTrackerStatusMap.containsKey(status.getTaskTrackerID())) {
			// Update last seen time
			status.setLastHeartbeatTime(System.currentTimeMillis());
			taskTrackerToTaskTrackerStatusMap.put(status.getTaskTrackerID(), status);
		}
	}

	/**
	 * Process the contained tasks and any jobs that might be affected based on
	 * the hearbeat message from a task tracker.
	 */
	public synchronized void updateTaskStatuses(TaskTrackerStatus taskTrackerStatus) {
		for (TaskStatus taskStatus : taskTrackerStatus.getTaskStatusList()) {
			TaskAttemptID taskAttemptID = taskStatus.getTaskAttemptID();
			JobInProgress jobInProgress = jobMap.get(taskStatus.getTaskAttemptID().getTaskInProgressID().getJobID());

			if (jobInProgress == null) {
				Log.warn("Get states for unknown job in progress");
				return;
			}

			TaskInProgress taskInProgress = taskAttemptToTaskInProgressMap.get(taskAttemptID);
			if (taskInProgress != null) {
				State oldJobInProgressState = jobInProgress.getState().clone();
				jobInProgress.updateTaskStatus(taskInProgress, taskStatus);
				State newJobInProgressState = jobInProgress.getState().clone();
				// State changed, update updateJobInProgressListeners
				if (!oldJobInProgressState.equals(newJobInProgressState)) {
					if (newJobInProgressState.isFinish()) {
						// Job finished
						taskScheduler.updateJob(jobInProgress, JobScheduleEvent.JOB_FINISHED);
					} else if (newJobInProgressState.isFailed()) {
						// Job failed
						taskScheduler.updateJob(jobInProgress, JobScheduleEvent.JOB_FAILED);
					}
				}
			} else {
				Log.warn("Get states for unknown task in progress");
			}
		}
		return;
	}

	/**
	 * Add new task tracker
	 * 
	 * @param taskTrackerStatus
	 *            The status for the new task tracker
	 */
	private synchronized void addNewTaskTracker(TaskTrackerStatus taskTrackerStatus) {
		if (!taskTrackerToTaskTrackerStatusMap.containsKey(taskTrackerStatus.getTaskTrackerID())) {
			// Update last seen time
			taskTrackerStatus.setLastHeartbeatTime(System.currentTimeMillis());
			taskTrackerToTaskTrackerStatusMap.put(taskTrackerStatus.getTaskTrackerID(), taskTrackerStatus);
			// Update total task slots
			totalMapSlots += taskTrackerStatus.getMapTaskSlotNum();
			totalReduceSlots += taskTrackerStatus.getReduceTaskSlotNum();
		}
	}

	/**
	 * Lost a task tracker, update task tracker related data structure
	 * 
	 * @param taskTrackerStatus
	 *            The status for the lost task tracker
	 */
	private synchronized void lostTaskTracker(TaskTrackerStatus taskTrackerStatus) {
		if (taskTrackerToTaskTrackerStatusMap.containsKey(taskTrackerStatus.getTaskTrackerID())) {
			// Handle task attempt on this task tracker
			List<TaskAttemptID> taskAttemptIDList = taskTrackerToTaskAttemptIDsMap.get(taskTrackerStatus
					.getTaskTrackerID());
			for (TaskAttemptID taskAttemptID : taskAttemptIDList) {
				TaskInProgress taskInProgress = taskAttemptToTaskInProgressMap.get(taskAttemptID);
				JobInProgress jobInProgress = taskInProgress.getJobInProgress();
				// Job is assigned to this task tracker
				if (jobInProgress.getState().isRunning()) {
					// Not completed task attempt or completed map task attempt
					// need to be marked failed
					boolean needToRestart = taskInProgress.isMap() || taskInProgress.getState().isInit()
							|| taskInProgress.getState().isRunning() || taskInProgress.getState().isPartialfailed();
					if (needToRestart) {
						// Mark the tip failed in the jip to triger task attempt
						// restart
						jobInProgress.failTask(taskInProgress, taskAttemptID, "Task tracker lost");
					}
				}
			}

			// Clear taskTracker related map
			taskTrackerToTaskTrackerStatusMap.remove(taskTrackerStatus.getTaskTrackerID());
			// Update total task slots
			totalMapSlots -= taskTrackerStatus.getMapTaskSlotNum();
			totalReduceSlots -= taskTrackerStatus.getReduceTaskSlotNum();
		}
	}

	public void initialize() throws Exception {
		// Init context
		Configuration conf = new Configuration();
		this.context = new JobTrackerContext(conf);

		// Check hostname
		this.context.checkJobTrackerHost();

//		// Check dir
//		//FileSystem fs = FileSystem.getFileSystem(context.getFileSystem());
//
//		fs.mkdir(new Path(context.getJobTrackerRoot()));

		// Initialize variables
		this.jobMap = new HashMap<JobID, JobInProgress>();
		this.taskAttemptToTaskInProgressMap = new HashMap<TaskAttemptID, TaskInProgress>();
		this.taskTrackerToHeartbeatResponseMap = new HashMap<TaskTrackerID, HeartbeatResponse>();
		this.taskTrackerToTaskAttemptIDsMap = new HashMap<TaskTrackerID, List<TaskAttemptID>>();
		this.taskTrackerToTaskTrackerStatusMap = new HashMap<TaskTrackerID, TaskTrackerStatus>();

		// Set Scheduler, default FIFOTaskScheduler
		// TODO read from conf Scheduler type
		if (this.taskScheduler == null) {
			this.taskScheduler = new FIFOTaskScheduler(context.getConf());
		}
		this.taskScheduler.setTaskTrackerManager(this);
		this.taskScheduler.start();

		// Set task tracker monitors
		this.expireTaskTrackers = new ExpireTaskTrackers();
		this.expireTaskTrackersThread = new Thread(expireTaskTrackers, "expireTaskTrackers");
		this.expireTaskTrackersThread.start();

		// Start RMI Service
		JobTrackerRMI stub = (JobTrackerRMI) UnicastRemoteObject.exportObject(this, conf.getInt("JobTrackerPort"));
		Registry registry = LocateRegistry.getRegistry();
		registry.rebind(JobTrackerRMI.RMIName, stub);
		Naming.rebind("JobTracker", this);

		Log.write("Jobtracker Ready");
	}

	private class ExpireTaskTrackers implements Runnable {

		public void run() {
			long interval = context.getTaskTrackerExpireInterval();
			while (true) {
				try {
					Thread.sleep(interval / 2);

					//
					// Loop through all expired items in the queue
					//
					// Need to lock the JobTracker here since we are
					// manipulating it's data-structures via
					// ExpireTrackers.run -> JobTracker.lostTaskTracker ->
					// JobInProgress.failedTask ->
					// JobTracker.markCompleteTaskAttempt
					// Also need to lock JobTracker before locking 'taskTracker'
					// 'trackerExpiryQueue' to prevent deadlock:

					synchronized (JobTracker.this) {
						synchronized (taskTrackerToTaskTrackerStatusMap) {
							long now = System.currentTimeMillis();
							for (TaskTrackerStatus lastStatus : taskTrackerToTaskTrackerStatusMap.values()) {
								if (now - lastStatus.getLastHeartbeatTime() > interval) {
									lostTaskTracker(lastStatus);
								}
							}
						}

					}
				} catch (InterruptedException e) {
					Log.write("ExpireTaskTracker thread interrupted");
					break;
				} catch (Exception e) {
					e.printStackTrace();
					Log.warn("ExpireTaskTracker error");
					break;
				}
			}
		}

	}

	/**
	 * Maintain lookup tables; called by JobInProgress and TaskInProgress
	 */
	public synchronized void createTaskEntry(TaskAttemptID taskAttemptID, TaskInProgress taskInProgress,
			TaskTrackerID taskTrackerID) {
		// TODO not fully finished
		// Log.write("Add task attempt" + taskAttemptID + "to task in progress"
		// + taskInProgress.getTaskInProgressID());

		// taracker --> taskAttemptID
		List<TaskAttemptID> taskAttemptIDList = taskTrackerToTaskAttemptIDsMap.get(taskTrackerID);
		if (taskAttemptIDList == null) {
			taskAttemptIDList = new ArrayList<TaskAttemptID>();
			taskTrackerToTaskAttemptIDsMap.put(taskTrackerID, taskAttemptIDList);
		}
		taskAttemptIDList.add(taskAttemptID);

		// taskAttemptID --> TIP
		taskAttemptToTaskInProgressMap.put(taskAttemptID, taskInProgress);
	}

	public void removeTaskEntry(TaskAttemptID taskAttemptID) {
	}

	/**
	 * TaskTrackerManager Interface function. Used by the TaskScheduler
	 */
	public synchronized void initJob(JobInProgress jobInProgress) {
		if (jobInProgress == null) {
			return;
		}
		try {
			Log.write("Initializing job " + jobInProgress.getJobID().toString());
			jobInProgress.initTaskInProgresses();
		} catch (Exception e) {
			e.printStackTrace();
			Log.write("Initializing " + jobInProgress.getJobID() + "failed");
		}
	}

	/**
	 * TaskTrackerManager Interface function. Used by the TaskScheduler
	 */
	public synchronized void failJob(JobInProgress jobInProgress) {
		if (jobInProgress == null) {
			return;
		}
		Log.write("Failing job " + jobInProgress.getJobID());
		jobInProgress.fail("Task scheduler command");
	}

	public synchronized ClusterStatus getClusterStatus() {
		return new ClusterStatus(totalMapSlots, totalReduceSlots);
	}

	public synchronized List<JobStatus> getJobTrackerStatus() throws RemoteException {
		List<JobStatus> jobStatusList = new ArrayList<JobStatus>();
		for (JobInProgress jobInProgress : jobMap.values()) {
			jobStatusList.add(jobInProgress.getJobStatus());
		}
		return jobStatusList;

	}

	public synchronized boolean killJob(JobID jobID) throws RemoteException {
		JobInProgress jobInProgress = jobMap.get(jobID);
		if (jobInProgress != null && jobInProgress.getState().isRunning()) {
			jobInProgress.kill();
			return true;
		} else {
			return false;
		}
	}

	public void shutdown() throws RemoteException {
		// TODO Auto-generated method stub

	}
}
