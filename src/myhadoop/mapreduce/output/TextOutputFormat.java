package myhadoop.mapreduce.output;

import myhadoop.job.JobContext;

public class TextOutputFormat<KEYOUT, VALUEOUT> extends OutputFormat<KEYOUT, VALUEOUT> {

	@Override
	public RecordWriter<KEYOUT, VALUEOUT> getRecordWriter(JobContext jobConf) {
		return new LineRecordWriter<KEYOUT, VALUEOUT>();
	}

}
