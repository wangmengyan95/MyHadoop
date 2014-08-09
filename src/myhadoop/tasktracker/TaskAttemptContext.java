package myhadoop.tasktracker;

import myhadoop.utility.Configuration;
import myhadoop.utility.Context;

/**
 * This class is a wrapper of configuration file. It provides some useful
 * function to get task attempt related configuration.
 */
public class TaskAttemptContext extends Context{	
	public TaskAttemptContext(Configuration conf) {
		super(conf);
	}
	
	public int getEOFSize() {
		return conf.getInt("EOFSize");
	}
}
