package myhadoop.mapreduce.input;

import java.io.IOException;
import java.util.ArrayList;

public class RecordSetReader<KEYIN extends Comparable<KEYIN>, VALUEIN> {
	
	IntermediateRecordReader<KEYIN, VALUEIN> reader;
	
	KEYIN bufferKey;
	ArrayList<VALUEIN> bufferValues;
	
	boolean init;
	
	public RecordSetReader(IntermediateRecordReader<KEYIN, VALUEIN> _reader) {
		reader = _reader;
		bufferKey = null;
		init = true;
	}
	
	public void initialize(int nRecs) {
		reader.initialize(nRecs);
	}

	public boolean nextKeyValue() throws IOException {
		return bufferKey != null || (init && reader.nextKeyValue());
	}

	public KEYIN getCurrentKey() throws IOException {
		if (init) {
			bufferKey = reader.getCurrentKey();
			init = false;
		}
		KEYIN ret = bufferKey;
		KEYIN tmp = null;
		bufferValues = new ArrayList<VALUEIN>();
		bufferValues.add(reader.getCurrentValue());
		while (reader.nextKeyValue() && (tmp = reader.getCurrentKey()).equals(bufferKey)) {
			bufferValues.add(reader.getCurrentValue());
		}
		bufferKey = tmp;
		return ret;
	}

	public Iterable<VALUEIN> getCurrentValues() {
		return bufferValues;
	}

	public double getProgress() {
		return reader.getProgress();
	}

	public void close() throws IOException {
		reader.close();
	}
}
