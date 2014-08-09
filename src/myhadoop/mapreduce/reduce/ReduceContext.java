package myhadoop.mapreduce.reduce;

import java.io.IOException;

import myhadoop.job.JobContext;
import myhadoop.mapreduce.input.RecordSetReader;
import myhadoop.mapreduce.output.RecordWriter;
import myhadoop.tasktracker.RecordMerger;
import myhadoop.tasktracker.Task;
import myhadoop.utility.Log;

public class ReduceContext<KEYIN extends Comparable<KEYIN>, VALUEIN, KEYOUT, VALUEOUT> {
	RecordMerger<KEYIN, VALUEIN> merger;
	RecordSetReader<KEYIN, VALUEIN> reader;
	RecordWriter<KEYOUT, VALUEOUT> writer;
	JobContext jobConf;
	Task task;
	
	double last = 0.0; // TODO
	double cur = 0.0;
	double intv = 0.15;
	
	public ReduceContext(JobContext _jobConf, Task _task, RecordMerger<KEYIN, VALUEIN> _merger, RecordSetReader<KEYIN, VALUEIN> _reader, RecordWriter<KEYOUT, VALUEOUT> _writer) {
		jobConf = _jobConf;
		task = _task;
		merger = _merger;
		reader = _reader;
		writer = _writer;
	}
	
	public boolean canRun() {
		boolean canRun = true;
		synchronized (task) {
			canRun = task.canRun();
		}
		return canRun;
	}
	
	public int fetchAndMerge() {
		return merger.fetchAndMergeRecords();
	}
	
	public boolean nextKeyValue() throws IOException {
		boolean ret = reader.nextKeyValue();
		cur = reader.getProgress();
		// Display and set the progress.
		if (cur - last > intv || !ret) {
			synchronized (task) {
				if (!ret)
					cur = 1.0;
				task.setProgress(cur);
				Log.write(task.toString() + " Progress: " + Math.round(cur * 100) + "%");
				last = cur;
			}
		}
		return ret;
	}
	
	public KEYIN getCurrentKey() throws IOException {
		return reader.getCurrentKey();
	}
	
	public Iterable<VALUEIN> getCurrentValues() {
		return reader.getCurrentValues();
	}
	
	public void write(KEYOUT key, VALUEOUT value) throws IOException {
		writer.write(key, value);
	}
}
