package myhadoop.jobtracker;

import myhadoop.tasktracker.Task;

public class LaunchAction extends TaskTrackerAction {
	private Task task;

	public LaunchAction(Task task) {
		this.task = task;
	}

	public Task getTask() {
		return task;
	}
}
