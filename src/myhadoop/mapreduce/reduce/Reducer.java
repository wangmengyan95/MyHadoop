package myhadoop.mapreduce.reduce;

import java.io.IOException;

/**
 * Reducer class. All reducers should extend the class.
 */
public abstract class Reducer<KEYIN extends Comparable<KEYIN>, VALUEIN, KEYOUT, VALUEOUT> {
	
	public void run(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) throws IOException {
		setup(context);
		boolean canRun = true;
		while (context.nextKeyValue() && canRun) {
			// For each <key, values>, 
			// Run map method implemented by user.
			reduce(context.getCurrentKey(), context.getCurrentValues(), context);
			canRun = context.canRun();
		}
		cleanup(context);
	}
	
	public void setup(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) throws IOException {
		// Set up the task.
		// Fetch and merge records from mappers.
		int nRecs = context.fetchAndMerge();
		if (nRecs > 0)
			context.reader.initialize(nRecs);
		else
			throw new IOException("Reduce: Fetch and merge intermediate files error.");
	}
	
	public void cleanup(ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) {
		// Clean up the task.
		// Do nothing.
	}
	
	public abstract void reduce(KEYIN key, Iterable<VALUEIN> values, ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context) throws IOException;
}
