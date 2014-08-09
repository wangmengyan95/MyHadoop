package myhadoop.tasktracker;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;

import myhadoop.job.JobContext;
import myhadoop.mapreduce.input.InputFormat;
import myhadoop.mapreduce.input.InputSplit;
import myhadoop.mapreduce.input.RecordReader;
import myhadoop.mapreduce.map.MapContext;
import myhadoop.mapreduce.map.Mapper;
import myhadoop.mapreduce.output.MapOutputCollector;
import myhadoop.mapreduce.output.RecordWriter;
import myhadoop.mapreduce.output.ReduceOutputCollector;
import myhadoop.utility.PathGenerator;

public class MapTaskRunner<KEYIN, VALUEIN, KEYOUT extends Comparable<KEYOUT>, VALUEOUT> extends TaskRunner {

	public MapTaskRunner(MapTask _task, RunningJob _job) {
		super(_task, _job);
	}
	
	@Override
	@SuppressWarnings({ "unchecked", "deprecation", "resource" })
	public void launchTask() throws Exception {		
		// Initialize Mapper and InputFormat instances.
		JobContext jobConf = job.getJobConf();
		
		File file = new File(PathGenerator.getJobRoot(task) + "/");
		URL[] urls = new URL[]{file.toURL()};
	    ClassLoader cl = new URLClassLoader(urls);
	    Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper = (Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>) cl.loadClass(job.getJobConf().getMapperClassName()).newInstance();
	    
		InputFormat<KEYIN, VALUEIN> inputFormat = 
				(InputFormat<KEYIN, VALUEIN>) Class.forName(jobConf.getInputFormatClassName()).newInstance();
		inputFormat.initialize(jobConf);
		
		// Get the split.
		InputSplit split = ((MapTask) task).getSplitInfo().getInputSplit();
		
		// Initialize the RecordReader.
		RecordReader<KEYIN, VALUEIN> reader = inputFormat.createRecordReader(split);
		
		// Output collector.
		RecordWriter<KEYOUT, VALUEOUT> writer = null;
		if (jobConf.getReduceTaskNum() == 0) {
			// No reducers, directly output the results.
			writer = new ReduceOutputCollector<KEYOUT, VALUEOUT>(jobConf, task);
		} else {
			// There are reducers, use MapOutputCollector to collect records.
			writer = new MapOutputCollector<KEYOUT, VALUEOUT>(jobConf, task);
		}
		
		// Initialize the MapContext.
		MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context = new MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(jobConf, task, reader, writer);
		
		// Run the mapper.
		mapper.run(context);
		
		reader.close();
		writer.close();
	}

}
