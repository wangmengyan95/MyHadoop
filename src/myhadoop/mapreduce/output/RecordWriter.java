package myhadoop.mapreduce.output;

import java.io.IOException;

public abstract class RecordWriter<KEYOUT, VALUEOUT> {
	public abstract void write(KEYOUT key, VALUEOUT value) throws IOException;
	public abstract void close() throws IOException;
}
