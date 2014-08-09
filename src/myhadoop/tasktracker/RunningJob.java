package myhadoop.tasktracker;

import java.util.HashSet;

import myhadoop.fs.FileSystem;
import myhadoop.fs.Path;
import myhadoop.job.JobContext;
import myhadoop.job.JobID;
import myhadoop.utility.Configuration;

public class RunningJob {
	boolean localized;
	JobID jobID;
	HashSet<Task> tasks;
	JobContext jobConf;
	
	public RunningJob(JobID _jobID) {
		localized = false;
		tasks = new HashSet<Task>();
	}
	
	public void setLocalized() {
		localized = true;
	}
	
	public void addTask(Task _task) {
		tasks.add(_task);
	}
	
	// Load context from a file in local file system.
	public void loadContextFromLocalFile(Path path) {
		FileSystem localFs = FileSystem.getFileSystem("local");
		jobConf = new JobContext(new Configuration(localFs, path));
	}
	
	public JobContext getJobConf() {
		return jobConf;
	}
}