package myhadoop.scheduler;

import myhadoop.utility.Configuration;
import myhadoop.utility.Context;

/**
 * This class is a wrapper of configuration file. It provides some useful
 * function to get job initialization listener configuration.
 */
public class JobInitializationListenerContext extends Context{

	public JobInitializationListenerContext(Configuration conf) {
		super(conf);
	}
	
	public int getMaxJobInitThreadNum() {
		return conf.getInt("MaxJobInitThreadNum");
	}
	
	public void setMaxJobInitThreadNum(int num) {
		conf.add("MaxJobInitThreadNum", num);
	}
}
