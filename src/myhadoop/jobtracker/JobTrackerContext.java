package myhadoop.jobtracker;

import java.net.InetAddress;
import java.net.UnknownHostException;

import myhadoop.utility.Configuration;
import myhadoop.utility.Context;

/**
 * This class is a wrapper of configuration file. It provides some useful
 * function to get job tracker related configuration.
 */
public class JobTrackerContext extends Context {

	public JobTrackerContext(Configuration conf) {
		super(conf);
	}

	public Long getTaskTrackerExpireInterval() {
		return conf.getLong("TaskTrackerExpireInterval") * 1000;
	}

	public void checkJobTrackerHost() throws UnknownHostException {
		String host = InetAddress.getLocalHost().getHostAddress().toString();
		// TODO
	}

	public String getJobTrackerHost() {
		return conf.getString("JobTrackerHost");
	}

	public int getJobTrackerPort() {
		return conf.getInt("JobTrackerPort");
	}

	public String getJobTrackerRoot() {
		return conf.getString("JobTrackerRoot");
	}

	public Long getMaxJobRunningTime() {
		return conf.getLong("MaxJobRunningTime") * 1000;
	}

	public int getMaxTaskRetryNum() {
		return conf.getInt("MaxTaskRetryNum");
	}
}
