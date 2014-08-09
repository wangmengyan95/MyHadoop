package myhadoop.jobtracker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import myhadoop.job.JobID;
import myhadoop.jobtracker.JobInProgress;
import myhadoop.jobtracker.JobTracker;
import myhadoop.mapreduce.input.SplitInfo;
import myhadoop.tasktracker.MapTask;
import myhadoop.tasktracker.ReduceTask;
import myhadoop.tasktracker.Task;
import myhadoop.tasktracker.TaskAttemptID;
import myhadoop.tasktracker.TaskStatus;
import myhadoop.tasktracker.TaskTrackerID;
import myhadoop.tasktracker.TaskTrackerStatus;
import myhadoop.utility.Log;

/*************************************************************
 * TaskInProgress maintains all the info needed for a Task in the lifetime of
 * its owning Job. A given Task might be re-executed, so we need a level of
 * indirection above the running-id itself. A given TaskInProgress contains
 * multiple taskids, 0 or more of which might be executing at any one time. A
 * taskid is now *never* recycled. A TIP allocates enough taskids to account for
 * all the failures it will ever have to handle. Once those are up, the TIP is
 * dead. **************************************************************
 */
public class TaskInProgress {
	private TaskInProgressID taskInProgressID;

	private int taskAttempNumber;
	private TaskAttemptID successfulTaskAttemptID;
	private Map<TaskAttemptID, TaskStatus> taskAttemptStatusMap;

	private long startTime;
	private long execStartTime;
	private long execFinishedTime;

	private SplitInfo splitInfo;

	private State state;

	private boolean isMap;

	private int RETRY_TIME = 2;

	// Used to maintain the lookup table in the jobtracker
	private JobTracker jobTracker;

	// Used to allow taskInProgress to quick access to its job
	// Used in expireLaunchingTask thread.
	private JobInProgress jobInProgress;

	// List recorded the tasktracker host which have run this task in progress.
	private List<TaskTrackerID> taskTrackerList;

	/**
	 * State of the task in progress, inner use, keep read and set state
	 * synchronized
	 */
	public class State {
		public static final String INIT = "INIT";
		// Some task attempts are running
		public static final String RUNNING = "RUNNING";
		// One of the task attempts has finished
		public static final String FINISH = "FINISH";
		// Some task attempts have failed, but not exceed the limit in
		// configuration
		public static final String PARTIALFAILED = "PARTIALFAILED";
		// The number of failed task attempt exceeds the limit in configuration
		public static final String FAILED = "FAILED";
		public static final String KILLED = "KILLED";
		private String stateStr = INIT;

		public synchronized boolean isInit() {
			return stateStr.equals(INIT);
		}

		public synchronized boolean isRunning() {
			return stateStr.equals(RUNNING);
		}

		public synchronized boolean isFinish() {
			return stateStr.equals(FINISH);
		}

		public synchronized boolean isFailed() {
			return stateStr.equals(FAILED);
		}

		public synchronized boolean isPartialfailed() {
			return stateStr.equals(PARTIALFAILED);
		}

		public synchronized boolean isKilled() {
			return stateStr.equals(KILLED);
		}

		public synchronized void setState(String state) {
			this.stateStr = state;
		}
	}

	/**
	 * 
	 * @param jobID
	 * @param isMap
	 * @param partition
	 *            means No ? parition of the whole map/reduce tasks
	 */
	public TaskInProgress(JobID jobID, boolean isMap, int partition, JobTracker jobTracker, JobInProgress jobInProgress) {
		this.taskInProgressID = new TaskInProgressID(jobID, isMap, partition);
		this.taskAttempNumber = 0;
		this.taskAttemptStatusMap = new HashMap<TaskAttemptID, TaskStatus>();
		this.startTime = System.currentTimeMillis();
		this.state = new State();
		this.isMap = isMap;
		this.jobTracker = jobTracker;
		this.taskTrackerList = new ArrayList<TaskTrackerID>();
		this.jobInProgress = jobInProgress;
		this.RETRY_TIME = jobTracker.getContext().getMaxTaskRetryNum();
	}

	public TaskInProgress(JobID jobID, boolean isMap, int partition, SplitInfo splitInfo, JobTracker jobTracker,
			JobInProgress jobInProgress) {
		this(jobID, isMap, partition, jobTracker, jobInProgress);
		this.splitInfo = splitInfo;
	}

	public TaskStatus getTaskStatus(TaskAttemptID taskAttemptID) {
		return taskAttemptStatusMap.get(taskAttemptID);
	}

	public SplitInfo getSplitInfo() {
		return splitInfo;
	}

	public void setSplitInfo(SplitInfo splitInfo) {
		this.splitInfo = splitInfo;
	}

	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}

	public TaskInProgressID getTaskInProgressID() {
		return taskInProgressID;
	}

	public JobInProgress getJobInProgress() {
		return jobInProgress;
	}

	public boolean isMap() {
		return isMap;
	}

	public TaskAttemptID getSuccessfulTaskAttemptID() {
		return successfulTaskAttemptID;
	}

	public void setSuccessfulTaskAttemptID(TaskAttemptID successfulTaskAttemptID) {
		this.successfulTaskAttemptID = successfulTaskAttemptID;
	}

	/**
	 * Return a Task that can be sent to a TaskTracker for execution.
	 */
	public synchronized Task getTaskToRun(TaskTrackerStatus taskTrackerStatus) {
		// Calculate attempt ID
		TaskAttemptID taskAttemptID = new TaskAttemptID(taskInProgressID, taskAttempNumber);
		taskAttempNumber += 1;
		// Update state
		state.setState(State.RUNNING);
		// Maintain the lookup table in jobtracker
		jobTracker.createTaskEntry(taskAttemptID, this, taskTrackerStatus.getTaskTrackerID());
		// Add to taskTracker list and map
		taskTrackerList.add(taskTrackerStatus.getTaskTrackerID());
		// Generate Task
		Task task = null;
		if (isMap) {
			task = new MapTask(taskAttemptID, isMap, taskTrackerStatus.getTaskTrackerID(), splitInfo);
		} else {
			task = new ReduceTask(taskAttemptID, isMap, taskTrackerStatus.getTaskTrackerID(), this.jobInProgress.getSuccessfulMapTaskToTaskTrackerIDMap());
		}
		taskAttemptStatusMap.put(taskAttemptID, task.getStatus());
		return task;
	}

	/**
	 * A status message from a client has arrived. It updates the status of a
	 * single component-thread-task, which might result in an overall
	 * TaskInProgress status update.
	 */
	public synchronized boolean updateStatus(TaskStatus taskStatus) {
		TaskStatus oldTaskStatus = taskAttemptStatusMap.get(taskStatus.getTaskAttemptID());
		// Safety check
		// already finished failed or killed task, changed can not be true
		// If TIP has failed, no need to update task attempt
		if (state.isFailed() || state.isKilled()) {
			return false;
		}

		boolean changed = oldTaskStatus == null || !oldTaskStatus.getState().equals(taskStatus.getState());
		if (changed) {
			// Task Attempt finished. This also means the TIP has finished
			if (taskStatus.getState().isFinished()) {
				this.state.setState(State.FINISH);
				execFinishedTime = System.currentTimeMillis();
				successfulTaskAttemptID = taskStatus.getTaskAttemptID();
				Log.write("Task attempt " + taskStatus.getTaskAttemptID() + " has finished");
			}
			// Task Attempt failed. This also means the TIP has partial finished
			// and should reschedule
			else if (taskStatus.getState().isFailed()) {
				this.state.setState(State.PARTIALFAILED);
				Log.warn("Task attempt " + taskStatus.getTaskAttemptID() + " has failed");
				// Exceed max retry time, task failed
				if (taskAttempNumber > RETRY_TIME) {
					this.state.setState(State.FAILED);
					execFinishedTime = System.currentTimeMillis();
					Log.warn("Task in progress " + taskInProgressID + " has failed");
				}
			}
		}
		taskAttemptStatusMap.put(taskStatus.getTaskAttemptID(), taskStatus);
		return changed;
	}

	public synchronized List<TaskStatus> getTaskAttemptStatusList() {
		List<TaskStatus> taskAttemptStatusList = new ArrayList<TaskStatus>();
		for (TaskStatus taskAttemptStatus : taskAttemptStatusMap.values()) {
			taskAttemptStatusList.add(taskAttemptStatus.clone());
		}
		return taskAttemptStatusList;
	}

	public synchronized List<TaskTrackerID> getTaskTrackerList() {
		return taskTrackerList;
	}

	public synchronized TaskTrackerID getSuccessfulTaskTrackerID() {
		if (successfulTaskAttemptID == null) {
			return null;
		} else {
			return taskAttemptStatusMap.get(successfulTaskAttemptID).getTaskTrackerID();
		}
	}
	
	public synchronized void kill() {
		if (state.isRunning()) {
			for (TaskAttemptID taskAttemptID : taskAttemptStatusMap.keySet()) {
				TaskStatus taskStatus = taskAttemptStatusMap.get(taskAttemptID);
				if (taskStatus.getState().isRunning() || taskStatus.getState().isUnassigned()) {
					taskStatus.setState(TaskStatus.State.KILLED);
				}
			}
		}
	}
}
