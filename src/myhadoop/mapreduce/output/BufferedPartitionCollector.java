package myhadoop.mapreduce.output;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import myhadoop.fs.FSDataOutputStream;
import myhadoop.fs.FileSystem;
import myhadoop.fs.Path;
import myhadoop.job.JobContext;
import myhadoop.tasktracker.Task;
import myhadoop.utility.Log;
import myhadoop.utility.PathGenerator;

public class BufferedPartitionCollector<KEYOUT extends Comparable<KEYOUT>, VALUEOUT> extends RecordWriter<KEYOUT, VALUEOUT> {
	
	JobContext jobConf;
	Task task;
	int partitionNum;
	
//	// TODO currently do not consider memory overflow.
//	int parts;
//	static int MAX_SIZE_IN_MEMORY = 32768;
	
	String root;
	
	ArrayList<Record<KEYOUT, VALUEOUT>> recordList = new ArrayList<Record<KEYOUT, VALUEOUT>>();
	FileSystem fs;
	
	public BufferedPartitionCollector(JobContext _jobConf, Task _task, int _partitionNum) {
		jobConf = _jobConf;
		task = _task;
		partitionNum = _partitionNum;
		
//		parts = 0;
		
		root = PathGenerator.getTaskRoot(task);
		fs = FileSystem.getFileSystem("local");
		fs.mkdir(new Path(root));
	}
	
	static class Record<K, V> {
		K key;
		V value;
		
		Record(K _key, V _value) {
			key = _key;
			value = _value;
		}
	}
	
	static class RecordComparator<K extends Comparable<K>, V> implements Comparator<Record<K, V>> {
		public int compare(Record<K, V> o1, Record<K, V> o2) {
			return o1.key.compareTo(o2.key);
		}
	}

	@Override
	public void write(KEYOUT key, VALUEOUT value) {
		recordList.add(new Record<KEYOUT, VALUEOUT>(key, value));
	}
	
	public void sortAndFlush() throws IOException {
		Collections.sort(recordList, new RecordComparator<KEYOUT, VALUEOUT>());
		String fileName = root + "/partition_" + partitionNum;
		
		FSDataOutputStream output = fs.create(new Path(fileName));
		ObjectOutputStream objOutput = new ObjectOutputStream(output);
		for(Record<KEYOUT, VALUEOUT> r : recordList) {
			objOutput.writeObject(r.key);
			objOutput.writeObject(r.value);
		}
		Log.write(recordList.size() + " records shuffled.");
		objOutput.close();
		output.close();
	}

	@Override
	public void close() throws IOException {
		sortAndFlush();
	}

}
