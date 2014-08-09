package myhadoop.tasktracker;

import myhadoop.job.JobContext;
import myhadoop.mapreduce.input.IntermediateRecordReader;
import myhadoop.mapreduce.input.RecordSetReader;
import myhadoop.mapreduce.output.RecordWriter;
import myhadoop.mapreduce.output.ReduceOutputCollector;
import myhadoop.mapreduce.reduce.ReduceContext;
import myhadoop.mapreduce.reduce.Reducer;

public class ReduceTaskRunner<KEYIN extends Comparable<KEYIN>, VALUEIN, KEYOUT, VALUEOUT> extends TaskRunner {

	public ReduceTaskRunner(ReduceTask _task, RunningJob _job) {
		super(_task, _job);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void launchTask() throws Exception {
		// Fetch all parts from TaskTrackers
		JobContext jobConf = job.getJobConf();
		Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> reducer = (Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>) Class.forName(jobConf.getReducerClassName()).newInstance();
		
		// Merge all parts
		RecordMerger<KEYIN, VALUEIN> merger = new RecordMerger<KEYIN, VALUEIN>(jobConf, (ReduceTask) task);
		RecordSetReader<KEYIN, VALUEIN> reader = new RecordSetReader<KEYIN, VALUEIN>(new IntermediateRecordReader<KEYIN, VALUEIN>(jobConf, task));
		RecordWriter<KEYOUT, VALUEOUT> writer = new ReduceOutputCollector<KEYOUT, VALUEOUT>(jobConf, task);
		
		// Initialize the reduceContext;
		ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context = new ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(jobConf, task, merger, reader, writer);
		
		// Run the reducer
		reducer.run(context);
		
		// Close the writer
		writer.close();
	}
}
