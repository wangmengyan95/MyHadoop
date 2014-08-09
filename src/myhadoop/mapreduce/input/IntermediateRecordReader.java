package myhadoop.mapreduce.input;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;

import myhadoop.job.JobContext;
import myhadoop.tasktracker.Task;
import myhadoop.tasktracker.TaskAttemptContext;
import myhadoop.utility.PathGenerator;

public class IntermediateRecordReader<KEYIN extends Comparable<KEYIN>, VALUEIN> extends RecordReader<KEYIN, VALUEIN> {
	
	JobContext context;
	Task task;
	ObjectInputStream input;
	
	KEYIN key;
	VALUEIN value;
	
	Integer nRecs;
	Integer cur;
	
	public IntermediateRecordReader(JobContext _context, Task _task) throws FileNotFoundException, IOException {
		context = _context;
		task = _task;
	}
	
	public void initialize(int _nRecs) {
		nRecs = _nRecs;
		initialize(null, null);
	}

	@Override
	public void initialize(TaskAttemptContext context, InputSplit inputSplit) {
		try {
			input = new ObjectInputStream(new FileInputStream(PathGenerator.getTaskRoot(task) + "/merged"));
			cur = 0;
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean nextKeyValue() throws IOException {
		try {
			key = (KEYIN) input.readObject();
			value = (VALUEIN) input.readObject();
			cur++;
			return key != null && value != null;
		} catch (Exception e) {
			return false;
		}	
	}

	/*
	 * Before getting current key, must call nextKeyValue to update the key and value.
	 */
	@Override
	public KEYIN getCurrentKey() {
		return key;
	}

	@Override
	public VALUEIN getCurrentValue() {
		return value;
	}

	@Override
	public double getProgress() {
		double p = cur * 1.0 / nRecs;
		return Math.min(1.0, p);
	}

	@Override
	public void close() throws IOException {
		input.close();
	}
}
