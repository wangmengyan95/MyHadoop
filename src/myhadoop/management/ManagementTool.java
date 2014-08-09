package myhadoop.management;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

import myhadoop.job.JobID;
import myhadoop.jobtracker.JobInProgress.State;
import myhadoop.jobtracker.JobStatus;
import myhadoop.jobtracker.JobTrackerRMI;
import myhadoop.tasktracker.TaskStatus;
import myhadoop.tasktracker.TaskTrackerID;
import myhadoop.utility.Configuration;

public class ManagementTool {
	public static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");

	public static void main(String[] str) throws RemoteException, NotBoundException {
		// RMI initialize
		ManagementToolContext context = new ManagementToolContext(new Configuration());
		Registry registry = LocateRegistry.getRegistry(context.getJobTrackerHost());
		JobTrackerRMI jobTrackerRMI = (JobTrackerRMI) registry.lookup(JobTrackerRMI.RMIName);

		Scanner reader = new Scanner(System.in);
		while (true) {
			System.out.println("Please input command");
			System.out.println("1. Check status");
			System.out.println("2. Kill job");

			String commandStr = reader.nextLine();
			String[] commands = commandStr.split(" ");

			List<JobStatus> jobStatusList = jobTrackerRMI.getJobTrackerStatus();
			if (commands != null && commands.length == 1 && commands[0].equals("1")) {
				printJobStatusList(jobStatusList);
			} else if (commands != null && commands.length == 1 && commands[0].equals("2")) {
				// Print kill job list
				System.out.println("Please input the Job ID you want to kill (such as 1, 2, 3):");
				List<JobID> jobIDSet = new ArrayList<JobID>();
				List<Integer> jobIDNumSet = new ArrayList<Integer>();
				for (JobStatus jobStatus : jobStatusList) {
					if (jobStatus.getState().equals(State.RUNNING)) {
						System.out.println("Job ID " + jobStatus.getJobID());
						jobIDSet.add(jobStatus.getJobID());
						jobIDNumSet.add(jobStatus.getJobID().getID());
					}
				}

				// Job st
				commandStr = reader.nextLine();
				commands = commandStr.split(" ");
				if (commands != null && commands.length == 1) {
					try {
						int jobID = Integer.valueOf(commands[0]);
						int index = jobIDNumSet.indexOf(jobID);
						if (index >= 0) {
							jobTrackerRMI.killJob(jobIDSet.get(index));
						}
						else {
							System.out.println("Pleas input valid ID");
						}
					} catch (Exception e) {
						System.out.println("Pleas input valid ID");
					}
				}

				System.out.println();
			} else {
				System.out.println("Please input vaild command");
			}
		}

	}

	public static void printJobStatusList(List<JobStatus> jobStatusList) {
		System.out.println("Job Number:" + jobStatusList.size());
		for (JobStatus jobStatus : jobStatusList) {
			System.out.println("----------------------------------------------------");
			System.out.println("Job ID:" + jobStatus.getJobID().getName() + " " + jobStatus.getJobID().getID());
			System.out.println("Job State:" + jobStatus.getState());

			long startTime = jobStatus.getStartTime();
			if (startTime > 0) {
				System.out.println("Start Time:" + printTime(jobStatus.getStartTime()));
			}
			long launchTime = jobStatus.getLaunchTime();
			if (launchTime > 0) {
				System.out.println("Launch Time:" + printTime(jobStatus.getLaunchTime()));
			}
			long finishTime = jobStatus.getFinishTime();
			if (finishTime > 0) {
				System.out.println("Finish Time:" + printTime(jobStatus.getFinishTime()));
			}
			System.out.println("Running Map Task Number:" + jobStatus.getRunningMapTaskNum());
			System.out.println("Running Reduce Task Number:" + jobStatus.getRunningReduceTaskNum());
			System.out.println("Finished Map Task Number:" + jobStatus.getFinishedMapTaskNum());
			System.out.println("Finished Reduce Task Number:" + jobStatus.getFinishedReduceTaskNum());

			List<TaskStatus> taskStatusList = jobStatus.getTaskStatusList();
			for (int i = 0; i <= taskStatusList.size() - 1; i++) {
				TaskStatus taskStatus = taskStatusList.get(i);
				TaskTrackerID taskTrackerID = taskStatus.getTaskTrackerID();
				System.out.println("******************************");
				String task = taskStatus.isMap() ? "Map" : "Reduce";
				System.out.println("Task ID:" + task + " "
						+ taskStatus.getTaskAttemptID().getTaskInProgressID().getPartition());
				System.out.println("Running On:" + taskTrackerID);
				System.out.println("Attempt Time:" + taskStatus.getTaskAttemptID().getTaskAttemptNumber());
				System.out.println("State:" + taskStatus.getState().getStateStr());
				startTime = taskStatus.getStartTime();
				if (startTime > 0) {
					System.out.println("Start Time:" + printTime(startTime));
				}
				finishTime = taskStatus.getFinishTime();
				if (finishTime > 0) {
					System.out.println("Finish Time:" + printTime(finishTime));
				}
			}
		}
		System.out.println();
		System.out.println();
	}

	public static String printTime(long time) {
		Date date = new Date(time);
		return simpleDateFormat.format(date);
	}
}
