package myhadoop.mapreduce.input;

import java.io.IOException;

import myhadoop.tasktracker.TaskAttemptContext;
import myhadoop.utility.Configuration;

public abstract class RecordReader<KEYIN, VALUEIN> {
	
	public abstract void initialize(TaskAttemptContext context, InputSplit inputSplit);

	/**
	 * Read the next key, value pair.
	 * @return true if a key/value pair was read
	 */
	public abstract boolean nextKeyValue() throws IOException;

	public abstract KEYIN getCurrentKey();

	public abstract VALUEIN getCurrentValue();

	/**
	 * The current progress of the record reader through its data.
	 * @return a number between 0.0 and 1.0 that is the fraction of the data
	 * read
	 */
	public abstract double getProgress();

	public abstract void close() throws IOException;
}
