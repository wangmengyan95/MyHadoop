package myhadoop.scheduler;

/**
 * This class stores the some status of the all task trackers which a job tracker
 * managed.
 * It is used for schedulers to assign tasks for task tracker.
 */
public class ClusterStatus {
	private int totalMapSlots;
	private int totalReduceSlots;

	public ClusterStatus(int totalMapSlots, int totalReduceSlots) {
		this.totalMapSlots = totalMapSlots;
		this.totalReduceSlots = totalReduceSlots;
	}

	public int getTotalMapSlots() {
		return totalMapSlots;
	}

	public int getTotalReduceSlots() {
		return totalReduceSlots;
	}
}
