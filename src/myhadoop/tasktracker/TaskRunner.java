package myhadoop.tasktracker;

import java.util.Calendar;

import myhadoop.utility.Log;

public abstract class TaskRunner implements Runnable {
	
	Boolean canRun;
	
	RunningJob job;
	Task task;
	
	public TaskRunner(Task _task, RunningJob _job) {
		task = _task;
		job = _job;
	}

	public void run() {
		try {
			// The task starts running.
			// Set the task as running.
			synchronized (task) {
				task.setStatus(TaskStatus.State.RUNNING);
				task.setStartTime(Calendar.getInstance().getTimeInMillis());
			}
			new Thread(new Timer()).start();
			Log.write(getTaskName() + " starts running.");
			launchTask();
			if (!task.canRun()) {
				throw new Exception("Task is terminated by task tracker.");
			}
			// The task is completed.
			// Set the task as succeed.
			synchronized (task) {
				task.setStatus(TaskStatus.State.FINISHED);
			}
			Log.write(getTaskName() + " is finished.");
		} catch (Exception e) {
			e.printStackTrace();
			// The task is failed.
			// Set the task as failed if exception happens.
			synchronized (task) {
				task.setStatus(TaskStatus.State.FAILED);
			}
			Log.write(getTaskName() + " is failed.");
		}
		synchronized (task) {
			task.setFinishTime(Calendar.getInstance().getTimeInMillis());
		}
	}
	
	public String getTaskName() {
		return task.toString();
	}
	
	/*
	 * Guarantee that the task does not exceed the required time.
	 */
	public class Timer implements Runnable {
		public static final int MAX_RUN_TIME = 60000;
		
		public void run() {
	    	long start = Calendar.getInstance().getTimeInMillis();
	    	long cur = start;
	    	
	    	while (cur < start + MAX_RUN_TIME) {
				try {
					Thread.sleep(MAX_RUN_TIME - (cur - start));
				} catch (InterruptedException e) {
				}
				cur = Calendar.getInstance().getTimeInMillis();
	    	}
			
			synchronized (task) {
				if (task.getStatus().getState().isRunning()) {
					task.terminate();
					Log.write("Task is terminated because the execution time exceeds " + MAX_RUN_TIME + " ms.");
				}
			}
		}
	}
	
	public abstract void launchTask() throws Exception;
}
