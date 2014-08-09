package myhadoop.mapreduce.output;

import myhadoop.job.JobContext;
import myhadoop.mapreduce.input.RecordReader;

public abstract class OutputFormat<KEYOUT, VALUEOUT> {
	public abstract RecordWriter<KEYOUT, VALUEOUT> getRecordWriter(JobContext jobConf);
}
