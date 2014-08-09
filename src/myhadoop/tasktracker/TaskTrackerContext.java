package myhadoop.tasktracker;

import java.net.InetAddress;
import java.net.UnknownHostException;

import myhadoop.utility.Configuration;
import myhadoop.utility.Context;

public class TaskTrackerContext extends Context {
	public TaskTrackerContext(Configuration conf) {
		super(conf);
	}

	public void setTaskTrackerHost() throws UnknownHostException {
		String host = InetAddress.getLocalHost().getHostAddress().toString();
		conf.add("TaskTrackerHost", host);
	}

	public String getTaskTrackerHost() {
		return conf.getString("TaskTrackerHost");
	}

	public void setTaskTrackerPort() {
		int port = ((int) Math.random()) * 10000;
		conf.add("TaskTrackerPort", String.valueOf(port));
	}

	public int getTaskTrackerPort() {
		return conf.getInt("TaskTrackerPort");
	}

	public String getTaskTrackerRoot() {
		return conf.getString("TaskTrackerRoot");
	}
	
	public String getJobTrackerHost() {
		return conf.getString("JobTrackerHost");
	}
	
	public int getMapTaskSlotNum() {
		return conf.getInt("MapTaskSlotNum");
	}
	
	public int getReduceTaskSlotNum() {
		return conf.getInt("ReduceTaskSlotNum");
	}

	public long getTaskTrackerHeartbeatInterval() {
		return conf.getLong("TaskTrackerHeartbeatInterval") * 1000;
	}
	
	public int getMaxTaskTrackerReconnectNum() {
		return conf.getInt("MaxTaskTrackerReconnectNum");
	}
}
