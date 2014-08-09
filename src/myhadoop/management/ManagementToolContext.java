package myhadoop.management;

import myhadoop.utility.Configuration;
import myhadoop.utility.Context;

public class ManagementToolContext extends Context{

	public ManagementToolContext(Configuration conf) {
		super(conf);
	}
	
	public String getJobTrackerHost() {
		return conf.getString("JobTrackerHost");
	}

	public int getJobTrackerPort() {
		return conf.getInt("JobTrackerPort");
	}
}
