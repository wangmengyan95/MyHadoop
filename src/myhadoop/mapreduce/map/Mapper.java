package myhadoop.mapreduce.map;

import java.io.IOException;

/**
 * Mapper class. All mappers should extend the class.
 */
public abstract class Mapper<KEYIN, VALUEIN, KEYOUT extends Comparable<KEYOUT>, VALUEOUT> {
	public void run(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) throws IOException {
		setup(context);
		boolean canRun = true;
		while (context.nextKeyValue() && canRun) {
			// For each <key, value>,
			// Run map method implemented by user.
			map(context.getCurrentKey(), context.getCurrentValue(), context);
			canRun = context.canRun();
		}
		cleanup(context);
	}
	
	public void setup(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) {
		// Set up the task. 
		// Do nothing.
	}
	
	public void cleanup(MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) {
		// Clean up the task.
		// Do nothing.
	}
	
	public abstract void map(KEYIN key, VALUEIN value, MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) throws IOException;
}
