package myhadoop.mapreduce.map;

import java.io.IOException;

import myhadoop.job.JobContext;
import myhadoop.mapreduce.input.RecordReader;
import myhadoop.mapreduce.output.RecordWriter;
import myhadoop.tasktracker.Task;
import myhadoop.utility.Log;

public class MapContext<KEYIN, VALUEIN, KEYOUT extends Comparable<KEYOUT>, VALUEOUT> {

	JobContext jobConf;
	Task task;
	RecordReader<KEYIN, VALUEIN> reader;
	RecordWriter<KEYOUT, VALUEOUT> writer;
	
	double last = 0.0; // TODO
	double cur = 0.0;
	double intv = 0.1;
	
	public MapContext(JobContext _jobConf, Task _task, RecordReader<KEYIN, VALUEIN> _reader, RecordWriter<KEYOUT, VALUEOUT> _writer) throws Exception {		
		jobConf = _jobConf;
		task = _task;
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
	
	public boolean nextKeyValue() throws IOException {
		boolean ret = reader.nextKeyValue();
		cur = reader.getProgress();
		// Display the progress.
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
	
	public KEYIN getCurrentKey() {
		return reader.getCurrentKey();
	}
	
	public VALUEIN getCurrentValue() {
		return reader.getCurrentValue();
	}
	
	public void write(KEYOUT key, VALUEOUT value) throws IOException {
		writer.write(key, value);
	}

}
