package myhadoop.jobtracker;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

import myhadoop.fs.Path;
import myhadoop.job.JobID;
import myhadoop.tasktracker.TaskTrackerStatus;

/*
 * This class is the RPC interface of the JobTrcker
 * Client will use this interface to submit task to
 * the JobTracker
 */
public interface JobTrackerRMI extends Remote{
	public static String RMIName = "JobTracker";
	public int getJobID() throws RemoteException;
	public int getTaskTrackerID() throws RemoteException;
	public void submitJob(Path path) throws RemoteException;
	public List<JobStatus> getJobTrackerStatus() throws RemoteException;
	public boolean killJob(JobID jobID) throws RemoteException;
	public void shutdown() throws RemoteException;
	public HeartbeatResponse submitHeartbeat(TaskTrackerStatus taskTrackerStatus, boolean canAskForNewTasks, boolean isFirstContact, int respondID) throws RemoteException;
}
