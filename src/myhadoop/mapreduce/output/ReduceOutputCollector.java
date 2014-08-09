package myhadoop.mapreduce.output;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import myhadoop.fs.FileSystem;
import myhadoop.fs.Path;
import myhadoop.job.JobContext;
import myhadoop.tasktracker.Task;

public class ReduceOutputCollector<KEYOUT, VALUEOUT> extends RecordWriter<KEYOUT, VALUEOUT> {

	JobContext jobConf;
	Task task;
	
	OutputStream out;
	BufferedWriter writer;
	
	FileSystem fs;
	
	public ReduceOutputCollector(JobContext _jobConf, Task _task) throws IOException {
		jobConf = _jobConf;
		task = _task;
		
		FileSystem fs = jobConf.getFileSystemInstance();
		fs.mkdir(new Path(jobConf.getOutputPath()));
		out = fs.create(new Path(jobConf.getOutputPath() + "/output_" + task.getPartition()));
		writer = new BufferedWriter(new OutputStreamWriter(out));
	}
	
	@Override
	public void write(KEYOUT key, VALUEOUT value) throws IOException {
		writer.write(key.toString() + "\t" + value.toString() + "\n");
	}

	@Override
	public void close() throws IOException {
		writer.flush();
		writer.close();
	}

}
