package myhadoop.tasktracker;

import java.io.Serializable;

import myhadoop.jobtracker.TaskInProgressID;

public class TaskAttemptID implements Serializable{
	private TaskInProgressID taskInProgressID;
	private int taskAttemptNumber;

	public TaskAttemptID(TaskInProgressID taskID, int taskAttemptNumber) {
		this.taskInProgressID = taskID;
		this.taskAttemptNumber = taskAttemptNumber;
	}

	public int getTaskAttemptNumber() {
		return taskAttemptNumber;
	}

	public TaskInProgressID getTaskInProgressID() {
		return taskInProgressID;
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("{taskInProgressID:" + taskInProgressID.toString() + "|");
		builder.append("taskAttemptNumber:" + String.valueOf(taskAttemptNumber)
				+ "}");
		return builder.toString();
	}

	public boolean equals(TaskAttemptID taskAttemptID) {
		return taskAttemptID.equals(taskAttemptID)
				&& taskAttemptNumber == taskAttemptID.getTaskAttemptNumber();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + taskAttemptNumber;
		result = prime
				* result
				+ ((taskInProgressID == null) ? 0 : taskInProgressID.hashCode());
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
		TaskAttemptID other = (TaskAttemptID) obj;
		if (taskAttemptNumber != other.taskAttemptNumber)
			return false;
		if (taskInProgressID == null) {
			if (other.taskInProgressID != null)
				return false;
		} else if (!taskInProgressID.equals(other.taskInProgressID))
			return false;
		return true;
	}
}
