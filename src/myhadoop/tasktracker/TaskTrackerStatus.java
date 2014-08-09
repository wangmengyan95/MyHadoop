package myhadoop.tasktracker;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Class recored the status of a task tracker
 * The task tracker send this class to job tracker by heartbeat
 *
 */
public class TaskTrackerStatus implements Serializable {
	private TaskTrackerID taskTrackerID;
	private String host;
	private int port;
	// Status of task running on this task tracker
	private List<TaskStatus> taskStatusList;
	private int mapTaskSlotNum;
	private int reduceTaskSlotNum;
	private volatile long lastHeartbeatTime;
	// Status of the resources on this task tracker
	private ResourceStatus resourceStatus;

	/**
	 * Class representing a collection of resources on this task tracker.
	 */
	static class ResourceStatus implements Serializable {
		private int numOfProcessors;
		private double systemLoadAverage;
		private long freePhysicalMemorySize;
		private long totalPhysicalMemorySize;

		public int getNumOfProcessors() {
			return numOfProcessors;
		}

		public void setNumOfProcessors(int numOfProcessors) {
			this.numOfProcessors = numOfProcessors;
		}

		public double getSystemLoadAverage() {
			return systemLoadAverage;
		}

		public void setSystemLoadAverage(double systemLoadAverage) {
			this.systemLoadAverage = systemLoadAverage;
		}

		public long getFreePhysicalMemorySize() {
			return freePhysicalMemorySize;
		}

		public void setFreePhysicalMemorySize(long freePhysicalMemorySize) {
			this.freePhysicalMemorySize = freePhysicalMemorySize;
		}

		public long getTotalPhysicalMemorySize() {
			return totalPhysicalMemorySize;
		}

		public void setTotalPhysicalMemorySize(long totalPhysicalMemorySize) {
			this.totalPhysicalMemorySize = totalPhysicalMemorySize;
		}

		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("{numOfProcessors: " + numOfProcessors + "|");
			builder.append("numOfProcessors: " + numOfProcessors + "|");
			builder.append("systemLoadAverage: " + systemLoadAverage + "|");
			builder.append("freePhysicalMemorySize: " + freePhysicalMemorySize + "|");
			builder.append("totalPhysicalMemorySize: " + totalPhysicalMemorySize + "}");
			return builder.toString();
		}
	}

	public TaskTrackerStatus() {
		this.taskStatusList = new ArrayList<TaskStatus>();
		this.resourceStatus = new ResourceStatus();
	}

	public TaskTrackerStatus(TaskTrackerID taskTrackerID, String host, int port, List<TaskStatus> taskStatusList,
			int maxMapTasks, int maxReduceTasks, long lastHeartbeatTime) {
		this(host, port, taskStatusList, maxMapTasks, maxReduceTasks, lastHeartbeatTime);
		this.taskTrackerID = taskTrackerID;
	}

	public TaskTrackerStatus(String host, int port, List<TaskStatus> taskStatusList, int maxMapTasks,
			int maxReduceTasks, long lastHeartbeatTime) {
		this.host = host;
		this.port = port;
		this.taskStatusList = taskStatusList;
		this.mapTaskSlotNum = maxMapTasks;
		this.reduceTaskSlotNum = maxReduceTasks;
		this.lastHeartbeatTime = lastHeartbeatTime;
		this.setResourceStatus(new ResourceStatus());
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public List<TaskStatus> getTaskStatusList() {
		return taskStatusList;
	}

	public void setTaskStatusList(List<TaskStatus> taskStatusList) {
		this.taskStatusList = taskStatusList;
	}

	public int getMapTaskSlotNum() {
		return mapTaskSlotNum;
	}

	public int getReduceTaskSlotNum() {
		return reduceTaskSlotNum;
	}

	public void setMaxReduceTasks(int maxReduceTasks) {
		this.reduceTaskSlotNum = maxReduceTasks;
	}

	public long getLastHeartbeatTime() {
		return lastHeartbeatTime;
	}

	public void setLastHeartbeatTime(long lastHeartbeatTime) {
		this.lastHeartbeatTime = lastHeartbeatTime;
	}

	public ResourceStatus getResourceStatus() {
		return resourceStatus;
	}

	public void setResourceStatus(ResourceStatus resourceStatus) {
		this.resourceStatus = resourceStatus;
	}

	public TaskTrackerID getTaskTrackerID() {
		return taskTrackerID;
	}

	public int countMapTasks() {
		return 0;
	}

	public int countReduceTasks() {
		return 0;
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("{taskTrackerID: " + taskTrackerID + "|");
		builder.append("mapTaskSlotNum: " + mapTaskSlotNum + "|");
		builder.append("reduceTaskSlotNum: " + reduceTaskSlotNum + "|");
		builder.append("lastHeartbeatTime: " + lastHeartbeatTime + "|");
		builder.append("{resourceStatus: " + resourceStatus.toString() + "}|");
		builder.append("{taskStatusList: " + taskStatusList + "|}");
		return builder.toString();
	}
}
