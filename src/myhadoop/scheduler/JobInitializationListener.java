package myhadoop.scheduler;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import myhadoop.jobtracker.JobInProgress;
import myhadoop.utility.Configuration;
import myhadoop.utility.Log;

/**
 * This class is the asyn initializing job on a job tracker. In order to
 * initialize a job, the job needs to download job configuration file from the
 * file system. This process may take times. In order to increase the throughput
 * of a job tracker, we use this class to asyn initialize a job. When a job is
 * submitted to a job tracker, it is added to this listener. This listener
 * manages a threadpool to asyn initializing job on it. After a job is
 * initialized, it is deleted from this listener. The state of the job is also
 * changed to RUN.
 * 
 */
public class JobInitializationListener {
	// Manage the job on this listener.
	private Queue<JobInProgress> jobInitQueue;
	// Thread pool which used to asyn initializing job.
	private ExecutorService threadPool;
	// A interface implemented by job tracker, the listener use the
	// taskTrackerManage.initJob()
	// to actually initialize a job
	private TaskTrackerManager taskTrackerManager;
	// Main thread of this listener
	private Thread jobInitManagerThread;

	private JobInitializationListenerContext context;

	/**
	 * This class is a runnable thread to initJob. It keeps removing the JIP
	 * from the jobInitQueue if possible and add the JIP to the threadPool to
	 * execute
	 */
	private class JobInitManager implements Runnable {

		public void run() {
			JobInProgress jobInProgress = null;
			while (true) {
				try {
					synchronized (jobInitQueue) {
						while (jobInitQueue.isEmpty()) {
							jobInitQueue.wait();
						}
						jobInProgress = jobInitQueue.remove();
					}
					threadPool.execute(new InitJobWrapper(jobInProgress));
				} catch (InterruptedException e) {
					Log.warn("JobInitManager is interrupted.");
					break;
				}
			}
			Log.warn("Close thread pool");
			threadPool.shutdown();
		}
	}

	/**
	 * This class is a wrapper of a initJob task. It wraps the
	 * taksTrackerManage.initJob() function. This wrapped is used to add to the
	 * threadPool
	 */
	private class InitJobWrapper implements Runnable {
		private JobInProgress jobInProgress;

		public InitJobWrapper(JobInProgress jobInProgress) {
			this.jobInProgress = jobInProgress;
		}

		public void run() {
			taskTrackerManager.initJob(jobInProgress);
		}
	}

	public JobInitializationListener(Configuration conf) {
		this.context = new JobInitializationListenerContext(conf);
	}

	public void start() {
		this.jobInitQueue = new LinkedList<JobInProgress>();
		this.threadPool = Executors.newFixedThreadPool(context.getMaxJobInitThreadNum());
		this.jobInitManagerThread = new Thread(new JobInitManager(), "JobInitManager");
		this.jobInitManagerThread.setDaemon(true);
		this.jobInitManagerThread.start();
	}

	public void terminate() {
		// Terminate the jobinitManager
		if (jobInitManagerThread != null && jobInitManagerThread.isAlive()) {
			jobInitManagerThread.interrupt();
			Log.write("Stop the JobInitManager");
		}
	}

	/**
	 * We add the JIP to the jobInitQueue, which is processed asynchronously to
	 * handle split-computation.
	 */
	public void jobAdded(JobInProgress job) {
		synchronized (jobInitQueue) {
			jobInitQueue.add(job);
			jobInitQueue.notifyAll();
		}

	}

	public void jobRemoved(JobInProgress job) {
		synchronized (jobInitQueue) {
			jobInitQueue.remove(job);
		}
	}

	public void jobUpdated(JobInProgress job, String jobEvent) {
		synchronized (jobInitQueue) {
			if (jobEvent.equals(JobScheduleEvent.JOB_FINISHED)) {
				jobInitQueue.remove(job);
			}
		}
	}

	public TaskTrackerManager getTaskTrackerManager() {
		return taskTrackerManager;
	}

	public void setTaskTrackerManager(TaskTrackerManager taskTrackerManager) {
		this.taskTrackerManager = taskTrackerManager;
	}

}
