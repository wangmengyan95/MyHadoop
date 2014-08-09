package myhadoop.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import myhadoop.jobtracker.JobInProgress;

/**
 * A JobQueueListener maintains the jobs being managed in a queue. By default
 * the queue is FIFO, but it is possible to use custom queue ordering by using
 * the constructor.
 */
public class JobQueueListener {
	private List<JobInProgress> jobInProgresseList;
	// JIP coomparator
	private Comparator<JobInProgress> comparator;

	public JobQueueListener() {
		// Default comparator, compare by ID, FIFO
		Comparator<JobInProgress> comparator = new Comparator<JobInProgress>() {

			public int compare(JobInProgress o1, JobInProgress o2) {
				return o1.getJobID().getID() - o2.getJobID().getID();
			}
		};
		this.comparator = comparator;
		List<JobInProgress> jobInProgresseList = new ArrayList<JobInProgress>();
		this.jobInProgresseList = Collections.synchronizedList(jobInProgresseList);
	}

	public JobQueueListener(Comparator<JobInProgress> comparator) {
		this.comparator = comparator;
		List<JobInProgress> jobInProgresseList = new ArrayList<JobInProgress>();
		this.jobInProgresseList = Collections.synchronizedList(jobInProgresseList);
	}

	/**
	 * Returns a synchronized view of the job queue.
	 */
	public synchronized List<JobInProgress> getJobQueue() {
		return jobInProgresseList;
	}

	public synchronized void jobAdded(JobInProgress jobInProgress) {
		jobInProgresseList.add(jobInProgress);
		// Resort the list
		Collections.sort(jobInProgresseList, comparator);
	}

	private synchronized void jobCompleted(JobInProgress jobInProgress) {
		if (!jobInProgresseList.contains(jobInProgress)) {
			throw new IllegalArgumentException("jobInProgress object does not exist in the list");
		} else {
			jobInProgresseList.remove(jobInProgress);
		}
	}

	public synchronized void jobUpdated(JobInProgress jobInProgress, String jobEvent) {
		if (jobEvent.equals(JobScheduleEvent.JOB_FINISHED) || jobEvent.equals(JobScheduleEvent.JOB_FAILED)) {
			jobCompleted(jobInProgress);
		}
	}
}
