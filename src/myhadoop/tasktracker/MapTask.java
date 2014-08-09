package myhadoop.tasktracker;

import myhadoop.mapreduce.input.SplitInfo;

public class MapTask extends Task{
	private SplitInfo splitInfo;
	
	public MapTask(TaskAttemptID taskAttemptID, boolean isMap, TaskTrackerID taskTrackerID, SplitInfo splitInfo) {
		super(taskAttemptID, isMap, taskTrackerID);
		this.setSplitInfo(splitInfo);
	}

	public SplitInfo getSplitInfo() {
		return splitInfo;
	}

	public void setSplitInfo(SplitInfo splitInfo) {
		this.splitInfo = splitInfo;
	}

}
