package myhadoop.tasktracker;

import java.io.Serializable;

import myhadoop.fs.Path;
import myhadoop.job.JobID;

public class Task implements Serializable{
	private TaskAttemptID taskAttemptID;

	private Path jobConfPath;
	
	private TaskStatus status;
	
	private boolean canRun = true;
	
	private boolean isMap;
	private long startTime;
	private long finishTime;
	
	private TaskTrackerID taskTrackerID;
	
	public Task(TaskAttemptID taskAttemptID, boolean isMap) {
		this.taskAttemptID = taskAttemptID;
		this.isMap = isMap;
		this.startTime = System.currentTimeMillis();
		this.status = new TaskStatus(taskAttemptID, isMap, startTime);
	}
	
	public Task(TaskAttemptID taskAttemptID, boolean isMap, TaskTrackerID taskTrackerID) {
		this.taskAttemptID = taskAttemptID;
		this.isMap = isMap;
		this.startTime = System.currentTimeMillis();
		this.status = new TaskStatus(taskAttemptID, isMap, startTime, taskTrackerID);
		this.taskTrackerID = taskTrackerID;
	}
	
	public TaskAttemptID getTaskAttemptID() {
		return taskAttemptID;
	}
	
	public boolean canRun() {
		return canRun;
	}
	
	public void terminate() {
		canRun = false;
	}
	
	public void resume() {
		canRun = true;
	}

	public Path getJobConfPath() {
		return jobConfPath;
	}

	public TaskStatus getStatus() {
		return status;
	}

	public long getStartTime() {
		return startTime;
	}

	public long getFinishTime() {
		return finishTime;
	}

	public void setTaskAttemptID(TaskAttemptID taskAttemptID) {
		this.taskAttemptID = taskAttemptID;
	}

	public void setJobConfPath(Path jobConfPath) {
		this.jobConfPath = jobConfPath;
	}


	public void setStatus(TaskStatus status) {
		this.status = status;
	}

	public void setMap(boolean isMap) {
		this.isMap = isMap;
	}

	public void setStartTime(long startTime) {
		synchronized (this.status) {
			this.status.setStartTime(startTime);
		}
		this.startTime = startTime;
	}

	public void setFinishTime(long finishTime) {
		synchronized (this.status) {
			this.status.setFinishTime(finishTime);
		}
		this.finishTime = finishTime;
	}
	
	public void setProgress(double d) {
		synchronized (this.status) {
			this.status.setProgress(d);
		}
	}

	public void setStatus(String state) {
		status.setState(state);
	}
	
	public boolean isMap() {
		return isMap;
	}
	
	public String getJobName() {
		return taskAttemptID.getTaskInProgressID().getJobID().getName();
	}
	
	public int getPartition() {
		return taskAttemptID.getTaskInProgressID().getPartition();
	}
	
	public int getAttemptNumber() {
		return taskAttemptID.getTaskAttemptNumber();
	}
	
	public JobID getJobID() {
		return taskAttemptID.getTaskInProgressID().getJobID();
	}
	
	public String toString() {
		return ((isMap() ? "Map" : "Reduce") + " task " + getPartition() + " (Att. " + getAttemptNumber() + ") of job " + getJobName());
	}

	public TaskTrackerID getTaskTrackerID() {
		return taskTrackerID;
	}

	public void setTaskTrackerID(TaskTrackerID taskTrackerID) {
		this.taskTrackerID = taskTrackerID;
	}
}
