package myhadoop.utility;

import myhadoop.tasktracker.Task;

public class PathGenerator {
	public static String getTaskRoot(Task task) {
		return "./tasks/job_" + task.getJobName() + "/" + (task.isMap() ? "map":"reduce") + "_part_" + task.getPartition() + "_att_" + task.getAttemptNumber();
	}
	
	public static String getJobRoot(Task task) {
		return "./tasks/job_" + task.getJobName();
	}
	
	public static String getClassFileName(String s) {
		int ind = s.lastIndexOf('.');
		return s.substring(ind + 1) + ".class";
	}
}
