package myhadoop.tasktracker;

import java.util.List;
import java.util.Map;

public class ReduceTask extends Task {
	private Map<TaskAttemptID, TaskTrackerID> mapTaskToTaskTrackerIDMap;

	public ReduceTask(TaskAttemptID taskAttemptID, boolean isMap, TaskTrackerID taskTrackerID,
			Map<TaskAttemptID, TaskTrackerID> mapTaskToTaskTrackerIDMap) {
		super(taskAttemptID, isMap, taskTrackerID);
		this.setMapTaskToTaskTrackerIDMap(mapTaskToTaskTrackerIDMap);
	}

	public Map<TaskAttemptID, TaskTrackerID> getMapTaskToTaskTrackerIDMap() {
		return mapTaskToTaskTrackerIDMap;
	}

	public void setMapTaskToTaskTrackerIDMap(Map<TaskAttemptID, TaskTrackerID> mapTaskToTaskTrackerIDMap) {
		this.mapTaskToTaskTrackerIDMap = mapTaskToTaskTrackerIDMap;
	}

}
