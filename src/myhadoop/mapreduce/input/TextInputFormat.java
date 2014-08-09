package myhadoop.mapreduce.input;

import myhadoop.job.JobContext;
import myhadoop.mapreduce.io.Text;
import myhadoop.tasktracker.TaskAttemptContext;
import myhadoop.utility.Configuration;

public class TextInputFormat extends FileInputFormat<Long, Text> {
	
	Configuration conf;
	
	@Override
	public RecordReader<Long, Text> createRecordReader(InputSplit split) {
		RecordReader<Long, Text> reader = new LineRecordReader();
		reader.initialize(new TaskAttemptContext(conf), split);
		return reader;
	}

	@Override
	public void initialize(JobContext context) {
		conf = context.getConf();
	}
	
}
