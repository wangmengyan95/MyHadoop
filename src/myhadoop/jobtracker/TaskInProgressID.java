package myhadoop.jobtracker;

import java.io.Serializable;

import myhadoop.job.JobID;

public class TaskInProgressID implements Serializable {
	private JobID jobID;
	private boolean isMap;
	private int partition;

	public TaskInProgressID(JobID jobID, boolean isMap, int partition) {
		this.jobID = jobID;
		this.isMap = isMap;
		this.partition = partition;
	}

	public boolean isMap() {
		return isMap;
	}

	public void setMap(boolean isMap) {
		this.isMap = isMap;
	}

	public int getPartition() {
		return partition;
	}

	public void setPartition(int partition) {
		this.partition = partition;
	}

	public JobID getJobID() {
		return jobID;
	}

	public void setJobID(JobID jobID) {
		this.jobID = jobID;
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("{jobID:" + jobID.toString() + "|");
		builder.append("isMap:" + String.valueOf(isMap) + "|");
		builder.append("partition:" + String.valueOf(partition) + "}");
		return builder.toString();
	}

	public boolean equals(TaskInProgressID taskInProgressID) {
		return jobID.equals(taskInProgressID.getJobID())
				&& isMap == taskInProgressID.isMap()
				&& partition == taskInProgressID.getPartition();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (isMap ? 1231 : 1237);
		result = prime * result + ((jobID == null) ? 0 : jobID.hashCode());
		result = prime * result + partition;
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
		TaskInProgressID other = (TaskInProgressID) obj;
		if (isMap != other.isMap)
			return false;
		if (jobID == null) {
			if (other.jobID != null)
				return false;
		} else if (!jobID.equals(other.jobID))
			return false;
		if (partition != other.partition)
			return false;
		return true;
	}
}
