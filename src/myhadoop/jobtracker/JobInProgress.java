package myhadoop.jobtracker;

import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import myhadoop.fs.FSDataInputStream;
import myhadoop.fs.FileSystem;
import myhadoop.fs.Path;
import myhadoop.job.JobContext;
import myhadoop.job.JobID;
import myhadoop.mapreduce.input.SplitInfo;
import myhadoop.tasktracker.Task;
import myhadoop.tasktracker.TaskAttemptID;
import myhadoop.tasktracker.TaskStatus;
import myhadoop.tasktracker.TaskTrackerID;
import myhadoop.tasktracker.TaskTrackerStatus;
import myhadoop.utility.Configuration;
import myhadoop.utility.Log;

/**
 * This class is the used by the job tracker to manage a job on a job tracker.
 * This class is created when a job is submitted to the job tracker.
 */
public class JobInProgress {
	private JobID jobID;
	private JobContext context;

	// State of this job in progress.
	private State state;

	private int mapTaskNum = 0;
	private int reduceTaskNum = 0;

	private int runningMapTaskNum = 0;
	private int runningReduceTaskNum = 0;
	private int finishedMapTaskNum = 0;
	private int finishedReduceTaskNum = 0;
	private int failedMapTaskNum = 0;
	private int failedReduceTaskNum = 0;

	private ArrayList<TaskInProgress> mapTaskList;
	private ArrayList<TaskInProgress> reduceTaskList;

	private long startTime;
	private long launchTime;
	private long finishTime;

	// Used to maintain the lookup table in the job tracker
	private JobTracker jobTracker = null;

	/**
	 * State of the task in progress, inner use, keep read and set state
	 * synchronized
	 */
	public class State implements Serializable{
		public static final String INIT = "INIT";
		public static final String RUNNING = "RUNNING";
		public static final String FINISH = "FINISH";
		public static final String FAILED = "FAILED";
		public static final String KILLED = "KILLED";
		private String stateStr = INIT;

		public synchronized boolean isInit() {
			return stateStr.equals(INIT);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			State other = (State) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (stateStr == null) {
				if (other.stateStr != null)
					return false;
			} else if (!stateStr.equals(other.stateStr))
				return false;
			return true;
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

		public synchronized boolean isKilled() {
			return stateStr.equals(KILLED);
		}
		
		public synchronized void setState(String state) {
			this.stateStr = state;
		}

		public synchronized State clone() {
			State cloendState = new State();
			cloendState.setState(stateStr);
			return cloendState;
		}

		public synchronized String getStateStr() {
			return stateStr;
		}

		private JobInProgress getOuterType() {
			return JobInProgress.this;
		}
	}

	public JobInProgress(FileSystem confFs, Path confPath, JobTracker jobTracker) {
		// Get job configuration from fs
		Configuration conf = new Configuration(confFs, confPath);
		context = new JobContext(conf);
		this.jobTracker = jobTracker;

		// Init list
		this.state = new State();
		this.jobID = new JobID(context.getJobID(), context.getJobName());
		this.setStartTime(System.currentTimeMillis());
		this.mapTaskList = new ArrayList<TaskInProgress>();
		this.reduceTaskList = new ArrayList<TaskInProgress>();
	}

	public State getState() {
		return state;
	}

	public JobID getJobID() {
		return jobID;
	}

	public void setJobID(JobID jobID) {
		this.jobID = jobID;
	}

	public int getMapTaskNum() {
		return mapTaskNum;
	}

	public int getReduceTaskNum() {
		return reduceTaskNum;
	}

	public int getFinishedMapTaskNum() {
		return finishedMapTaskNum;
	}

	public int getFinishedReduceTaskNum() {
		return finishedReduceTaskNum;
	}

	public long getLaunchTime() {
		return launchTime;
	}

	public long getFinishTime() {
		return finishTime;
	}

	/**
	 * Construct the splits, etc. This is invoked from an async thread so that
	 * split-computation doesn't block anyone.
	 * 
	 * @throws Exception
	 */
	public synchronized void initTaskInProgresses() throws Exception {
		// Read split file
		Path splitFile = new Path(context.getSplitInfoPath());
//		FileSystem fs = FileSystem.getFileSystem(context.getFileSystem());
		FileSystem fs = context.getFileSystemInstance();
		FSDataInputStream input = fs.open(splitFile);
		ObjectInputStream reader = new ObjectInputStream(input);
		ArrayList<SplitInfo> splitInfoList = (ArrayList<SplitInfo>) reader.readObject();
		Log.write("Finish read split job " + jobID + " Split num:" + splitInfoList.size());
		reader.close();

		// Set launch time
		launchTime = System.currentTimeMillis();

		// Create map task
		mapTaskNum = splitInfoList.size();
		context.setMapTaskNum(mapTaskNum);
		for (int i = 0; i <= mapTaskNum - 1; i++) {
			TaskInProgress taskInProgress = new TaskInProgress(jobID, true, i, splitInfoList.get(i), jobTracker,
					JobInProgress.this);
			mapTaskList.add(taskInProgress);
		}
		Log.write("Split path:" + context.getSplitInfoPath());
		Log.write(jobID.toString() + " get " + mapTaskNum + " map tasks");

		// Create reduce task
//		reduceTaskNum = Math.max(1, context.getMaxReduceTaskNumPerJob());
//		context.setReduceTaskNum(reduceTaskNum);
		reduceTaskNum = context.getReduceTaskNum();
		for (int i = 0; i <= reduceTaskNum - 1; i++) {
			TaskInProgress taskInProgress = new TaskInProgress(jobID, false, i, jobTracker, JobInProgress.this);
			reduceTaskList.add(taskInProgress);
		}
		Log.write(jobID.toString() + " get " + reduceTaskNum + " reduce tasks");

		// Change state
		state.setState(State.RUNNING);
		launchTime = System.currentTimeMillis();
	}

	public synchronized void fail(String reason) {
		this.state.setState(State.FAILED);
		Log.write("Job in progress " + jobID + " failed due to " + reason);
	}

	public synchronized void kill() {
		this.state.setState(State.KILLED);
		Log.write("Job in progress " + jobID + " has been killed");
		for (TaskInProgress mapTaskInProgress : mapTaskList) {
			mapTaskInProgress.kill();
		}
		for (TaskInProgress reduceTaskInProgress : reduceTaskList) {
			reduceTaskInProgress.kill();
		}
	}
	
	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	/**
	 * Assign map task to a given task tracker. This function is called by the
	 * task scheduler through the interface TaskTrackerManager
	 * 
	 * @param status
	 * @return
	 */
	public synchronized Task obtainNewMapTask(TaskTrackerStatus status) {
		if (!state.isRunning()) {
			Log.write("Job " + jobID + " is still not ready or has problem. Can not assign tasks for this job.");
			return null;
		}

		// Find a assignable task in progress
		TaskInProgress taskInProgress = findNewMapTask(status);
		Task task = null;
		if (taskInProgress != null) {
			task = taskInProgress.getTaskToRun(status);
			task.setJobConfPath(new Path(context.getJobConfigurationFilePath())); /* !!!! */

			// Update status
			runningMapTaskNum += 1;
		}
		return task;
	}

	private synchronized TaskInProgress findNewMapTask(TaskTrackerStatus status) {
		// Find a init local map task
		for (TaskInProgress taskInProgress : mapTaskList) {
			// Init TIP or Partialfailed TIP can be assiend to taskTracker
			// If the TIP has not started or the Task attempt has not failed on
			// this task tracker
			// Then the TIP can be assign to this task tracker
			boolean canAssigntoThisTaskTracker = taskInProgress.getState().isInit()
					|| (taskInProgress.getState().isPartialfailed() && !taskInProgress.getTaskTrackerList().contains(
							status.getTaskTrackerID()));
			// TODO
			canAssigntoThisTaskTracker = taskInProgress.getState().isInit()
					|| taskInProgress.getState().isPartialfailed();
			if (canAssigntoThisTaskTracker) {
				if (taskInProgress.getSplitInfo().getLocationList().contains(status.getHost())) {
					return taskInProgress;
				}
			}
		}

		// If not find a local map task, find a remote map task
		for (TaskInProgress taskInProgress : mapTaskList) {
			// Init TIP or Failed TIP can be assiend to taskTracker
			boolean canAssigntoThisTaskTracker = taskInProgress.getState().isInit()
					|| (taskInProgress.getState().isPartialfailed() && !taskInProgress.getTaskTrackerList().contains(
							status.getTaskTrackerID()));
			// TODO
			canAssigntoThisTaskTracker = taskInProgress.getState().isInit()
					|| taskInProgress.getState().isPartialfailed();
			if (canAssigntoThisTaskTracker) {
				return taskInProgress;
			}
		}

		// No map task can be assigned, return null
		return null;
	}

	/**
	 * Assign reduce task to a given task tracker. This function is called by
	 * the task scheduler through the interface TaskTrackerManager
	 * 
	 * @param status
	 * @return
	 */
	public synchronized Task obtainNewReduceTask(TaskTrackerStatus status) {
		if (!state.isRunning()) {
			Log.write("Job " + jobID + " is still not ready or has problem. Can not assign tasks for this job.");
			return null;
		}

		boolean canAssignReduceTask = canScheduleReduceTask();
		if (!canAssignReduceTask) {
			return null;
		}

		// Find a assignable task in progress
		TaskInProgress taskInProgress = findNewReduceTask(status);
		Task task = null;
		if (taskInProgress != null) {
			task = taskInProgress.getTaskToRun(status);

			// Update status
			runningReduceTaskNum += 1;
		}
		return task;
	}

	private synchronized TaskInProgress findNewReduceTask(TaskTrackerStatus status) {
		// Find a reduce task
		for (TaskInProgress taskInProgress : reduceTaskList) {
			// Init TIP or Failed TIP can be assiend to taskTracker
			boolean canAssigntoThisTaskTracker = taskInProgress.getState().isInit()
					|| (taskInProgress.getState().isPartialfailed() && !taskInProgress.getTaskTrackerList().contains(
							status.getTaskTrackerID()));
			// TODO
			canAssigntoThisTaskTracker = taskInProgress.getState().isInit()
					|| taskInProgress.getState().isPartialfailed();
			if (canAssigntoThisTaskTracker) {
				return taskInProgress;
			}
		}
		// No reduce task can be assigned, return null
		return null;
	}

	/**
	 * Judge when can assign reduce task for this job.
	 * 
	 * @return
	 */
	public synchronized boolean canScheduleReduceTask() {
		// Check whether all map tasks have been finished
		return finishedMapTaskNum >= mapTaskNum;
	}

	/**
	 * Update the status of this JIP by a given task status of this JIP
	 * 
	 * @param taskInProgress
	 * @param taskStatus
	 */
	public synchronized void updateTaskStatus(TaskInProgress taskInProgress, TaskStatus taskStatus) {
		// Handle task tracker temporily lost
		// If job has failed, no need to update task status
		if (state.isFailed() || state.isKilled()) {
			return;
		}
		TaskStatus oldTaskStatus = taskInProgress.getTaskStatus(taskStatus.getTaskAttemptID());
		taskInProgress.updateStatus(taskStatus);
		boolean changed = oldTaskStatus == null || !oldTaskStatus.getState().equals(taskStatus.getState());
		// Handle status changed
		if (changed) {
			// Task attempt finished
			if (taskStatus.getState().isFinished()) {
				// Running to finished
				if (oldTaskStatus.getState().isRunning()) {
					if (taskStatus.isMap()) {
						runningMapTaskNum--;
						finishedMapTaskNum++;
					} else {
						runningReduceTaskNum--;
						finishedReduceTaskNum++;
					}
					// Check whether the job has finished
					if (finishedMapTaskNum >= mapTaskNum && finishedReduceTaskNum >= reduceTaskNum) {
						finishTime = System.currentTimeMillis();
						state.setState(State.FINISH);
						Log.write("Job " + jobID + " has finished");
					}
				}
			}
			// Task attempt failed
			else if (taskStatus.getState().isFailed()) {
				// Running to failed
				if (oldTaskStatus.getState().isRunning()) {
					if (taskStatus.isMap()) {
						runningMapTaskNum--;
					} else {
						runningReduceTaskNum--;
					}
				}
				// Finished to failed (Map task when task tracker lost)
				else if (oldTaskStatus.getState().isFinished()) {
					if (taskStatus.isMap()) {
						finishedMapTaskNum--;
					}
				}

				// Check whether the TIP has failed
				if (taskInProgress.getState().isFailed()) {
					// Once one of the task in a job failed, the job is
					// failed
					if (taskInProgress.isMap()) {
						failedMapTaskNum++;
					} else {
						failedReduceTaskNum++;
					}
					state.setState(State.FAILED);
					Log.warn("Job " + jobID + " has failed");
				}
			}
		}

		// Recalculate progress
		// TODO
	}

	/**
	 * Fail a task attempt in a jip. This function will be called by the expire
	 * task tracker thread in the job tracker. When a running task tracker lost
	 * connection to the job tracker, some tasks on this task tracker should be
	 * failed and then this function will be triggered.
	 */
	public void failTask(TaskInProgress taskInProgress, TaskAttemptID taskAttemptID, String reason) {
		// Incompleted task status currently
		TaskStatus oldTaskStatus = taskInProgress.getTaskStatus(taskAttemptID);
		if (oldTaskStatus == null) {
			Log.warn("Got unknown taskAttemptID in failTask");
		} else {
			TaskStatus newTaskStatus = new TaskStatus(taskAttemptID, oldTaskStatus.isMap());
			Log.warn("Lost task attempt " + taskAttemptID + "because " + reason);
			newTaskStatus.setState(TaskStatus.State.FAILED);
			newTaskStatus.setStartTime(oldTaskStatus.getStartTime());
			newTaskStatus.setFinishTime(oldTaskStatus.getFinishTime());
			// Mark in the failed taskAttemptID in tip
			updateTaskStatus(taskInProgress, newTaskStatus);
		}
	}

	public synchronized JobStatus getJobStatus() {
		JobStatus jobStatus = new JobStatus(jobID);
		jobStatus.setStartTime(startTime);
		jobStatus.setLaunchTime(launchTime);
		jobStatus.setFinishTime(finishTime);
		jobStatus.setRunningMapTaskNum(runningMapTaskNum);
		jobStatus.setRunningReduceTaskNum(runningReduceTaskNum);
		jobStatus.setFinishedMapTaskNum(finishedMapTaskNum);
		jobStatus.setFinishedReduceTaskNum(finishedReduceTaskNum);

		jobStatus.setState(state.getStateStr());
		for (TaskInProgress mapTaskInProgress : mapTaskList) {
			jobStatus.addTaskStatusList(mapTaskInProgress.getTaskAttemptStatusList());
		}
		for (TaskInProgress reduceTaskInProgress : reduceTaskList) {
			jobStatus.addTaskStatusList(reduceTaskInProgress.getTaskAttemptStatusList());
		}

		return jobStatus;
	}
	
	public synchronized Map<TaskAttemptID, TaskTrackerID> getSuccessfulMapTaskToTaskTrackerIDMap() {
		if (finishedMapTaskNum < mapTaskNum) {
			return null;
		}
		Map<TaskAttemptID, TaskTrackerID> successfulMapTaskToTaskTrackerIDMap = new HashMap<TaskAttemptID, TaskTrackerID>();
		for (TaskInProgress taskInProgress : mapTaskList) {
			successfulMapTaskToTaskTrackerIDMap.put(taskInProgress.getSuccessfulTaskAttemptID(), taskInProgress.getSuccessfulTaskTrackerID());
		}
		return successfulMapTaskToTaskTrackerIDMap;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((jobID == null) ? 0 : jobID.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		JobInProgress other = (JobInProgress) obj;
		if (jobID == null) {
			if (other.jobID != null)
				return false;
		} else if (!jobID.equals(other.jobID))
			return false;
		return true;
	}
}
