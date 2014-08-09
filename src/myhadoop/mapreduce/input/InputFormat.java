package myhadoop.mapreduce.input;

import java.util.List;

import myhadoop.job.JobContext;

public abstract class InputFormat<K, V> {
	public abstract void initialize(JobContext context);
	public abstract List<InputSplit> getSplitList(JobContext context);
	public abstract RecordReader<K, V> createRecordReader(InputSplit split);
}