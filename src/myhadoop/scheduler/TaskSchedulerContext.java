package myhadoop.scheduler;

import myhadoop.utility.Configuration;
import myhadoop.utility.Context;

/**
 * This class is a wrapper of configuration file. It provides some useful
 * function to get task scheduler related configuration.
 */
public class TaskSchedulerContext extends Context {

	public TaskSchedulerContext(Configuration conf) {
		super(conf);
	}

}
