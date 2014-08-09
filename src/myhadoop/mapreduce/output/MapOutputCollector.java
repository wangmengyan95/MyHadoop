package myhadoop.mapreduce.output;

import java.io.IOException;
import java.util.ArrayList;

import myhadoop.job.JobContext;
import myhadoop.mapreduce.partition.Partitioner;
import myhadoop.tasktracker.Task;
import myhadoop.utility.Log;

public class MapOutputCollector<KEYOUT extends Comparable<KEYOUT>, VALUEOUT> extends RecordWriter<KEYOUT, VALUEOUT>{
	JobContext jobConf;
	Task task;
	
	int nParts;
	ArrayList<BufferedPartitionCollector<KEYOUT, VALUEOUT>> parts;
	
	Partitioner<KEYOUT, VALUEOUT> partitioner;
	
	public MapOutputCollector(JobContext _jobConf, Task _task) {
		jobConf = _jobConf;
		task = _task;
		
		nParts = jobConf.getReduceTaskNum();
		parts = new ArrayList<BufferedPartitionCollector<KEYOUT, VALUEOUT>>();
		for (int i = 0; i < nParts; i++)
			parts.add(new BufferedPartitionCollector<KEYOUT, VALUEOUT>(jobConf, task, i));
		partitioner = new Partitioner<KEYOUT, VALUEOUT>();
	}


	@Override
	public void write(KEYOUT key, VALUEOUT value) throws IOException {
		// Based on the partition number, write the record to corresponding partition collector.
		int partition = partitioner.getPartition(key, value, nParts);
		parts.get(partition).write(key, value);
	}

	@Override
	public void close() throws IOException {
		for (BufferedPartitionCollector<KEYOUT, VALUEOUT> p : parts)
			p.close();
	}

}
