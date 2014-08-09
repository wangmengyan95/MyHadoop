package myhadoop.tasktracker;

import java.io.Serializable;

public class TaskStatus implements Serializable {
	private TaskAttemptID taskAttemptID;
	private State state;
	private boolean isMap;
	private long startTime;
	private long finishTime;
	private double progress = 0.0;
	private TaskTrackerID taskTrackerID;

	/**
	 * State of the task, inner use, keep read and set state synchronized
	 */
	public class State implements Serializable {
		public static final String UNASSIGNED = "UNASSIGNED";
		public static final String RUNNING = "RUNNING";
		public static final String FINISHED = "FINISHED";
		public static final String FAILED = "FAILED";
		public static final String KILLED = "KILLED";
		private String stateStr = RUNNING;

		public synchronized boolean isUnassigned() {
			return stateStr.equals(UNASSIGNED);
		}

		public synchronized boolean isRunning() {
			return stateStr.equals(RUNNING);
		}

		public synchronized boolean isFinished() {
			return stateStr.equals(FINISHED);
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

		public synchronized String getStateStr() {
			return stateStr;
		}

		public boolean equals(State otherState) {
			return stateStr.equals(otherState.getStateStr());
		}
	}

	public TaskStatus(TaskAttemptID taskAttemptID, boolean isMap) {
		this.taskAttemptID = taskAttemptID;
		this.state = new State();
		this.isMap = isMap;
	}
	
	public TaskStatus(TaskAttemptID taskAttemptID, boolean isMap, long startTime) {
		this.taskAttemptID = taskAttemptID;
		this.state = new State();
		this.isMap = isMap;
		this.startTime = startTime;
	}
	
	public TaskStatus(TaskAttemptID taskAttemptID, boolean isMap, long startTime, TaskTrackerID taskTrackerID) {
		this.taskAttemptID = taskAttemptID;
		this.state = new State();
		this.isMap = isMap;
		this.startTime = startTime;
		this.taskTrackerID = taskTrackerID;
	}

	public State getState() {
		return state;
	}

	public void setState(String stateStr) {
		state.setState(stateStr);
	}

	public TaskAttemptID getTaskAttemptID() {
		return taskAttemptID;
	}

	public boolean isMap() {
		return isMap;
	}

	public void setMap(boolean isMap) {
		this.isMap = isMap;
	}

	public long getFinishTime() {
		return finishTime;
	}

	public void setFinishTime(long finishTime) {
		this.finishTime = finishTime;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}
	
	public void setProgress(double d) {
		progress = d;
	}
	
	public double getProgress() {
		return progress;
	}

	public TaskStatus clone() {
		TaskStatus clonedTaskStatus = new TaskStatus(taskAttemptID, isMap);
		clonedTaskStatus.setMap(isMap);
		clonedTaskStatus.setStartTime(startTime);
		clonedTaskStatus.setFinishTime(finishTime);
		clonedTaskStatus.setState(state.getStateStr());
		clonedTaskStatus.setTaskTrackerID(taskTrackerID);
		return clonedTaskStatus;
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("{taskAttemptID:" + taskAttemptID + "|");
		builder.append("state:" + state.getStateStr() + "|");
		builder.append("isMap:" + String.valueOf(isMap) + "}");
		return builder.toString();
	}

	public TaskTrackerID getTaskTrackerID() {
		return taskTrackerID;
	}

	public void setTaskTrackerID(TaskTrackerID taskTrackerID) {
		this.taskTrackerID = taskTrackerID;
	}

}
