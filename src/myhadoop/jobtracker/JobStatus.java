package myhadoop.jobtracker;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import myhadoop.job.JobID;
import myhadoop.jobtracker.JobInProgress.State;
import myhadoop.tasktracker.TaskStatus;
import myhadoop.tasktracker.TaskTrackerID;

public class JobStatus implements Serializable {
	private JobID jobID;
	private int mapTaskNum = 0;
	private int reduceTaskNum = 0;

	private int runningMapTaskNum = 0;
	private int runningReduceTaskNum = 0;
	private int finishedMapTaskNum = 0;
	private int finishedReduceTaskNum = 0;
	private int failedMapTaskNum = 0;
	private int failedReduceTaskNum = 0;

	private long startTime;
	private long launchTime;
	private long finishTime;

	private String state;

	private List<TaskStatus> taskStatusList;

	public JobStatus(JobID jobID) {
		this.jobID = jobID;
		this.taskStatusList = new ArrayList<TaskStatus>();
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

	public void setMapTaskNum(int mapTaskNum) {
		this.mapTaskNum = mapTaskNum;
	}

	public int getReduceTaskNum() {
		return reduceTaskNum;
	}

	public void setReduceTaskNum(int reduceTaskNum) {
		this.reduceTaskNum = reduceTaskNum;
	}

	public int getRunningMapTaskNum() {
		return runningMapTaskNum;
	}

	public void setRunningMapTaskNum(int runningMapTaskNum) {
		this.runningMapTaskNum = runningMapTaskNum;
	}

	public int getRunningReduceTaskNum() {
		return runningReduceTaskNum;
	}

	public void setRunningReduceTaskNum(int runningReduceTaskNum) {
		this.runningReduceTaskNum = runningReduceTaskNum;
	}

	public int getFinishedMapTaskNum() {
		return finishedMapTaskNum;
	}

	public void setFinishedMapTaskNum(int finishedMapTaskNum) {
		this.finishedMapTaskNum = finishedMapTaskNum;
	}

	public int getFinishedReduceTaskNum() {
		return finishedReduceTaskNum;
	}

	public void setFinishedReduceTaskNum(int finishedReduceTaskNum) {
		this.finishedReduceTaskNum = finishedReduceTaskNum;
	}

	public int getFailedMapTaskNum() {
		return failedMapTaskNum;
	}

	public void setFailedMapTaskNum(int failedMapTaskNum) {
		this.failedMapTaskNum = failedMapTaskNum;
	}

	public int getFailedReduceTaskNum() {
		return failedReduceTaskNum;
	}

	public void setFailedReduceTaskNum(int failedReduceTaskNum) {
		this.failedReduceTaskNum = failedReduceTaskNum;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getLaunchTime() {
		return launchTime;
	}

	public void setLaunchTime(long launchTime) {
		this.launchTime = launchTime;
	}

	public long getFinishTime() {
		return finishTime;
	}

	public void setFinishTime(long finishTime) {
		this.finishTime = finishTime;
	}

	public List<TaskStatus> getTaskStatusList() {
		return taskStatusList;
	}

	public void addTaskStatus(TaskStatus taskStatus) {
		taskStatusList.add(taskStatus);
	}

	public void addTaskStatusList(List<TaskStatus> taskStatusList) {
		this.taskStatusList.addAll(taskStatusList);
	}

	public String getState() {
		return state;
	}

	public void setState(String stateStr) {
		this.state = stateStr;
	}
}
