package myhadoop.jobtracker;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is the heartbeat response which a job tracker sends to a task
 * tracker.
 */
public class HeartbeatResponse implements Serializable {
	// ID to synchronized the job tracker and task tracker
	private int responseID;
	// A list of action the task tracker should do
	private List<TaskTrackerAction> actionList;

	public HeartbeatResponse(int responseID) {
		this.responseID = responseID;
		this.actionList = new ArrayList<TaskTrackerAction>();
	}

	public List<TaskTrackerAction> getActionList() {
		return actionList;
	}

	public void setActionList(List<TaskTrackerAction> actionList) {
		this.actionList = actionList;
	}

	public int getResponseID() {
		return responseID;
	}

	public void setResponseID(int responseID) {
		this.responseID = responseID;
	}

	public void addTaskTrackerAction(TaskTrackerAction action) {
		actionList.add(action);
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("responseID: " + responseID + "|");
		builder.append("actionList: " + actionList + "|");
		return builder.toString();
	}
}
