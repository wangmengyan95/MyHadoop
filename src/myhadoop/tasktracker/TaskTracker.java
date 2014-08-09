package myhadoop.tasktracker;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.FileAlreadyExistsException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import myhadoop.fs.FileSystem;
import myhadoop.fs.LocalFileSystem;
import myhadoop.fs.Path;
import myhadoop.job.JobID;
import myhadoop.jobtracker.HeartbeatResponse;
import myhadoop.jobtracker.JobTrackerRMI;
import myhadoop.jobtracker.KillAction;
import myhadoop.jobtracker.LaunchAction;
import myhadoop.jobtracker.TaskTrackerAction;
import myhadoop.mapreduce.io.IntWritable;
import myhadoop.mapreduce.io.Text;
import myhadoop.utility.Configuration;
import myhadoop.utility.FileOperator;
import myhadoop.utility.HostMonitor;
import myhadoop.utility.Log;
import myhadoop.utility.PathGenerator;
import myhadoop.utility.PortGenerator;

public class TaskTracker implements Runnable {
	
	private static final int MAPEVENTFETCHER_PORT = 3579;
	
	private TaskTrackerID taskTrackerID;
	private JobTrackerRMI jobTrackerRMI; // RMI interface provided by job
											// tracker
	private int heartbeatRespondID = -1; // Heartbeat counter
	private TaskTrackerStatus taskTrackerStatus = null; // Status report to the
														// tasktracker
	private State state = new State(); // State of the tasktracker, inner use
	private Map<TaskAttemptID, Task> taskMap; // Task ID to Task
	private TaskTrackerContext context;
	private long lastHearbeatTime = 0;
	private HostMonitor hostMonitor;
	
	private Integer nMapTasks;
	private Integer nReduceTasks;
	
	private FileSystem fs;
	
	private HashMap<JobID, RunningJob> runningJobs;
	
	private int reconnectNum = 0;

	/**
	 * State of the task tracker, inner use, keep read and set state
	 * synchronized
	 */
	private class State {
		public static final String INIT = "INIT";
		public static final String RUNNING = "RUNNING";
		public static final String STALE = "STALE";
		public static final String SHUTTING_DOWN = "SHUTTING_DOWN";
		private String state = RUNNING;

		public synchronized boolean isInit() {
			return state.equals(INIT);
		}

		public synchronized boolean isRunning() {
			return state.equals(RUNNING);
		}

		public synchronized boolean isStale() {
			return state.equals(STALE);
		}

		public synchronized boolean isShuttingDown() {
			return state.equals(SHUTTING_DOWN);
		}

		public synchronized void setState(String state) {
			this.state = state;
		}
	}

	/**
	 * The server retry loop. This while-loop attempts to connect to the
	 * JobTracker. It only loops when the old TaskTracker has gone bad (its
	 * state is stale somehow) and we need to reinitialize everything.
	 * Currently, the state becomes stale when get restart command from
	 * jobtracker
	 */
	public void run() {
		while (true) {
			if (state.isRunning()) {
				try {
					offerService();
				} catch (Exception e) {
					Log.warn("Error, lost connection to job tracker, try to re-connect");
					if (reconnectNum <= context.getMaxTaskTrackerReconnectNum()) {
						try {
							Thread.sleep(context.getTaskTrackerHeartbeatInterval());
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
						reconnectNum++;
					}
					else {
						shutdown();
						break;
					}
				}
			} else if (state.isStale()) {
				// If stale, try to restart
				try {
					initialize();
				} catch (Exception e) {
					Log.warn("Reinitialize failed");
					e.printStackTrace();
				}
			} else if (state.isShuttingDown()) {
				shutdown();
				break;
			}
		}
	}

	/**
	 * Main service loop. Will stay in this loop forever unless it is in stale
	 * state
	 * 
	 * @throws Exception
	 */
	private void offerService() throws Exception {
		while (true) {
			if (state.isRunning()) {
				// Wait sometime if not reach interval time
				long now = System.currentTimeMillis();
				long waitTime = context.getTaskTrackerHeartbeatInterval() - (now - lastHearbeatTime);
				if (waitTime > 0) {
					Thread.sleep(waitTime);
				}

				// Generate status message
				if (taskTrackerStatus == null) {
					synchronized (this) {
						taskTrackerStatus = new TaskTrackerStatus(taskTrackerID, context.getTaskTrackerHost(),
								context.getTaskTrackerPort(), getTaskStatusListFromMap(), context.getMapTaskSlotNum(),
								context.getReduceTaskSlotNum(), lastHearbeatTime);
					}
				}

				// Check whether can ask for new task
				// Currently just think about task numbers, do not think about
				// memory or other resource
				boolean canAskForNewTasks = false;
				synchronized (this) {
					canAskForNewTasks = (taskTrackerStatus.countMapTasks() <= context.getMapTaskSlotNum())
							&& (taskTrackerStatus.countReduceTasks() <= context.getReduceTaskSlotNum());
				}

				// Refresh resource status
				taskTrackerStatus.getResourceStatus().setNumOfProcessors(hostMonitor.getAvailableProcessors());
				taskTrackerStatus.getResourceStatus()
						.setFreePhysicalMemorySize(hostMonitor.getFreePhysicalMemorySize());
				taskTrackerStatus.getResourceStatus().setSystemLoadAverage(hostMonitor.getSystemLoadAverage());
				taskTrackerStatus.getResourceStatus().setTotalPhysicalMemorySize(
						hostMonitor.getTotalPhysicalMemorySize());

				boolean isFirstContact = lastHearbeatTime == 0;
				// Call RMI to send status to job tracker
				HeartbeatResponse response = jobTrackerRMI.submitHeartbeat(taskTrackerStatus, canAskForNewTasks,
						isFirstContact, heartbeatRespondID);

				heartbeatRespondID = response.getResponseID();
				lastHearbeatTime = System.currentTimeMillis();

				List<TaskTrackerAction> actionList = response.getActionList();
				if (actionList.size() > 0)
					Log.write("Got respond from job tracker:" + response.toString());	
				if (actionList != null) {
					for (TaskTrackerAction action : actionList) {
						if (action instanceof LaunchAction) {
							// Launch a task.
							LaunchAction launchAction = (LaunchAction) action;
							new Thread(new TaskLauncher(launchAction)).start();
						} else if (action instanceof KillAction) {
							killAll();
						} 
					}
				}

				// Reset taskTrackerStatus
				taskTrackerStatus = null;
			}
		}
	}

	private synchronized void initialize() throws Exception {
		// Init context
		Configuration conf = new Configuration();
		context = new TaskTrackerContext(conf);

		// Set host and port
		context.setTaskTrackerHost();
		context.setTaskTrackerPort();

		// Clear out old variable
		heartbeatRespondID = -1;
		taskTrackerStatus = null;
		state = new State();
		taskMap = new HashMap<TaskAttemptID, Task>();
		lastHearbeatTime = 0;
		hostMonitor = new HostMonitor();

		// RMI initialize
		Registry registry = LocateRegistry.getRegistry(context.getJobTrackerHost());
		jobTrackerRMI = (JobTrackerRMI) registry.lookup(JobTrackerRMI.RMIName);
		
		// Task launcher initialize
		nMapTasks = 0;
		nReduceTasks = 0;
		
		// FS initialize
		fs = context.getFileSystemInstance();
		
		// Jobs initialize
		runningJobs = new HashMap<JobID, RunningJob>();

		// Get task tracker ID
		int id = jobTrackerRMI.getTaskTrackerID();
		taskTrackerID = new TaskTrackerID(id, context.getTaskTrackerHost(), context.getTaskTrackerPort());
		Log.write("Establish connection with the job tracker. The task tracker's ID is " + id); 
		
		// mapEventsFetcher
		MapEventFetcher fetcher = new MapEventFetcher();
		new Thread(fetcher).start();
		
		Log.write("Initialization finished");
	}

	private void checkLocalDir() throws FileAlreadyExistsException {
		LocalFileSystem localFS = new LocalFileSystem();
		localFS.mkdir(new Path(context.getTaskTrackerRoot()));
	}

	private List<TaskStatus> getTaskStatusListFromMap() {
		List<TaskStatus> taskStatusList = new ArrayList<TaskStatus>();
		
		for (Task task : taskMap.values())
			taskStatusList.add(task.getStatus().clone());
		return taskStatusList;
	}

	private void shutdown() {
		killAll();
	}
	
	private void killAll() {
		for (Task t : taskMap.values()) {
			synchronized (t) {
				t.terminate();
			}
		}
	}
	
	/*
	 * Asynchronizely start a thread to run a new task.
	 */
	private class TaskLauncher implements Runnable {	
		LaunchAction action;
		
		public TaskLauncher(LaunchAction _action) {
			action = _action;
		}
		
		public void run() {
			Task task = registerTask(action, this);
			try {
				localizeJob(task);
			} catch (Exception e) {
				Log.write(e.getMessage());
			}
		}
		
	}
	
	/*
	 * Add the task into the task list.
	 */
	private Task registerTask(LaunchAction action, TaskLauncher launcher) {
		Task task = action.getTask();
		Log.write("Launch task: " + task.toString());
		
		synchronized(this) {
			taskMap.put(action.getTask().getTaskAttemptID(), action.getTask());
			if (task.isMap())
				nMapTasks++;
			else
				nReduceTasks++;
		}
		
		return action.getTask();
	}
	
	/*
	 * Localize corresponding job of the task.
	 */
	private void localizeJob(Task task) throws IOException {
		// Check whether the job is added
		String jobRoot = PathGenerator.getJobRoot(task);
		Path localJobConfPath = new Path(jobRoot + "/config");
		RunningJob job = addTaskToJob(task);
		
		// If not added, download and load the JobContext
		synchronized(job) {
			if (!job.localized) {
				// Download the config file.
				fs.copyToLocalFS(task.getJobConfPath(), localJobConfPath);
				job.loadContextFromLocalFile(localJobConfPath);
				// Download the jar file.
				Path jobClassPath = new Path(jobRoot + "/" + PathGenerator.getClassFileName(job.getJobConf().getJarClassName()));
				fs.copyToLocalFS(new Path(job.jobConf.getJarFilePath() + "/" + PathGenerator.getClassFileName(job.getJobConf().getJarClassName())), jobClassPath); 
				Path mapClassPath = new Path(jobRoot + "/" + PathGenerator.getClassFileName(job.getJobConf().getMapperClassName()));
				fs.copyToLocalFS(new Path(job.jobConf.getJarFilePath() + "/" + PathGenerator.getClassFileName(job.getJobConf().getMapperClassName())), mapClassPath); 
				if (job.getJobConf().getReduceTaskNum() > 0) {
					Path reduceClassPath = new Path(jobRoot + "/" + PathGenerator.getClassFileName(job.getJobConf().getReducerClassName()));
					fs.copyToLocalFS(new Path(job.jobConf.getJarFilePath() + "/" + PathGenerator.getClassFileName(job.getJobConf().getReducerClassName())), reduceClassPath); 
				}
			}
			job.setLocalized();
		}
		
		// Launch the task
		if (task.isMap())
			launchMapTaskForJob(task, job);
		else
			launchReduceTaskForJob(task, job);
	}
	
	/*
	 * Add a task to a running job, and return the RunningJob object.
	 */
	private RunningJob addTaskToJob(Task _task) {
		RunningJob job = null;
		JobID _jobID = _task.getJobID();
		synchronized(runningJobs) {
			if (!runningJobs.containsKey(_jobID)) {
				job = new RunningJob(_jobID);
				runningJobs.put(_jobID, job);
			} else {
				job = runningJobs.get(_jobID);
			}
			job.addTask(_task);
		}
		
		FileSystem local = FileSystem.getFileSystem("local");
		local.mkdir(new Path(PathGenerator.getTaskRoot(_task)));
		return job;
	}
	
	/*
	 * Launch a map task.
	 */
	private void launchMapTaskForJob(Task task, RunningJob job) {
		new Thread(new MapTaskRunner((MapTask) task, job)).start();
	}
	
	/*
	 * Launch a reduce task.
	 */
	private void launchReduceTaskForJob(Task task, RunningJob job) {
		new Thread(new ReduceTaskRunner((ReduceTask) task, job)).start();
	}
	
	/*
	 * Listen to requests from reducers for output generated by mapper.
	 */
	class MapEventFetcher implements Runnable {
		public void run() {
			try {
				ServerSocket server = new ServerSocket(MAPEVENTFETCHER_PORT);
				while (true) {
					Socket socket = server.accept();
					OutputStream out = socket.getOutputStream();
					out.flush();
					ObjectOutputStream output = new ObjectOutputStream(out);
					ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
					
					MapEventMessage message = (MapEventMessage) input.readObject();
					MapEventMessage ack = new MapEventMessage(MapEventMessage.MESSAGE_UNDEFINED);
					if (message.getType() == MapEventMessage.MESSAGE_FETCH_MAP_OUTPUT) {
						// Reducers want to get the intermediate records generated by mapper.
						if (!taskMap.containsKey(message.getAttemptID())) {
							ack.withType(MapEventMessage.MESSAGE_FETCH_MAP_OUTPUT_FAIL);
							output.writeObject(ack);
							socket.close();
						} else {
							Task task = taskMap.get(message.getAttemptID());
							String path = generateFilePath(task, message.getPartitionNum());
							File file = new File(path);
							if (file.exists()) {
								ack.withType(MapEventMessage.MESSAGE_FETCH_MAP_OUTPUT_ACK);
								output.writeObject(ack);
								new Thread(new MapOutputTransmitter(socket, path)).start();
							} else {
								ack.withType(MapEventMessage.MESSAGE_FETCH_MAP_OUTPUT_FAIL);
								output.writeObject(ack);
								socket.close();
							}
						}
					} else {
						ack.withType(MapEventMessage.MESSAGE_FETCH_MAP_OUTPUT_FAIL);
						output.writeObject(ack);
						socket.close();
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		private String generateFilePath(Task task, int partitionNum) {
			return PathGenerator.getTaskRoot(task)
					+ "/partition_" + partitionNum;
		}
	}
	
	/*
	 * Transmit the output file of mapper to reducer.
	 */
	class MapOutputTransmitter implements Runnable {
		Socket socket;
		String path;
		
		public MapOutputTransmitter(Socket _socket, String _path) {
			socket = _socket;
			path = _path;
		}
		
		public void run() {
			try {
				OutputStream out = socket.getOutputStream();
				FileOperator.sendFile(out, path);
				socket.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args){
		TaskTracker taskTracker = new TaskTracker();
		try {
			taskTracker.initialize();
			new Thread(taskTracker).start();
		} catch (Exception e) {
			Log.warn("Initialization error, please try again");
		}
	}
}
