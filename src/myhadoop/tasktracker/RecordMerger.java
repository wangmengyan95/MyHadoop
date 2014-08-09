package myhadoop.tasktracker;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

import myhadoop.job.JobContext;
import myhadoop.utility.FileOperator;
import myhadoop.utility.Log;
import myhadoop.utility.PathGenerator;

public class RecordMerger<KEYIN extends Comparable<KEYIN>, VALUEIN> {
	
	Integer nFinished;
	Boolean failed;
	JobContext jobConf;
	ReduceTask task;
	
	public RecordMerger(JobContext _jobConf, ReduceTask _task) {
		jobConf = _jobConf;
		task = _task;
		
		nFinished = 0;
		failed = false;
	}
	
	/*
	 * Fetch intermediate record from a mapper.
	 */
	class RecordFileFetcher implements Runnable {
		
		MapperInfo mapper;
		TaskAttemptID attemptID;
		
		public RecordFileFetcher(MapperInfo _mapper) {
			mapper = _mapper;
			attemptID = mapper.id;
		}
		
		public void run() {
			try {
				Socket socket = new Socket(mapper.host, mapper.port);
				ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
				output.flush();
				ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
				
				MapEventMessage message = new MapEventMessage(MapEventMessage.MESSAGE_FETCH_MAP_OUTPUT)
											.withAttemptID(attemptID)
											.withPartitionNum(task.getPartition());
				
				output.writeObject(message);
				MapEventMessage recv = (MapEventMessage) input.readObject();
				if (recv.getType() == MapEventMessage.MESSAGE_FETCH_MAP_OUTPUT_ACK) {
					String path = PathGenerator.getTaskRoot(task) + "/fetched_partition_" + mapper.seqNum;
					FileOperator.recvFile(socket.getInputStream(), path);
					Thread.sleep(1000);
					synchronized (nFinished) {
						nFinished++;
					}
				} else {
					synchronized (failed) {
						failed = true;
					}
				}
				socket.close();
			} catch (Exception e) {
				synchronized (failed) {
					failed = true;
				}
				e.printStackTrace();
			}
		}
	}
	
	class MapperInfo {	
		public String host;
		public int port;
		public int seqNum;
		TaskAttemptID id;
		
		public MapperInfo(String _host, int _port, int _seqNum, TaskAttemptID _id) {
			host = _host;
			port = _port;
			seqNum = _seqNum;
			id = _id;
		}
	}
	
	/*
	 * Fetch records from all mappers and merge the records.
	 */
	public int fetchAndMergeRecords() {
		ArrayList<MapperInfo> mappers = new ArrayList<MapperInfo>();
		int i = 0;
		for (TaskAttemptID id : task.getMapTaskToTaskTrackerIDMap().keySet()) {
			TaskTrackerID tid = task.getMapTaskToTaskTrackerIDMap().get(id);
			mappers.add(new MapperInfo(tid.getHost(), 3579, i++, id));
		}
		Log.write("Preparing to fetch intermediate records from " + mappers.size() + " mappers...");
		int size = mappers.size();
		
		for (MapperInfo mapper : mappers)
			new Thread(new RecordFileFetcher(mapper)).start();
		
		// Wait until all records are fetched.
		while (!failed) {
			synchronized(nFinished) {
				if (nFinished >= size)
					break;
			}
		}
		
		if (nFinished == size) {
			Log.write("All partitions fetched.");
			try {
				return mergeRecords(size);
			} catch (Exception e) {
				e.printStackTrace();
				return -1;
			}
		} else
			return -1;
	}
	
	static class Record<K, V> {
		K key;
		V value;
		int seqNum;
		
		Record(K _key, V _value, int _seqNum) {
			key = _key;
			value = _value;
			seqNum = _seqNum;
		}
	}
	
	static class RecordComparator<K extends Comparable<K>, V> implements Comparator<Record<K, V>> {
		public int compare(Record<K, V> o1, Record<K, V> o2) {
			return o1.key.compareTo(o2.key);
		}
	}
	
	@SuppressWarnings("unchecked")
	private Record<KEYIN, VALUEIN> readRecord(ObjectInputStream in, int seqNum) {
		try {
			return new Record<KEYIN, VALUEIN>((KEYIN) in.readObject(), (VALUEIN) in.readObject(), seqNum);
		} catch (Exception e) {
			return null;
		}
	}
	
	private int mergeRecords(int size) throws FileNotFoundException, IOException {
		// Merge sort the sorted records.
		int nRecs = 0;
		ObjectInputStream[] inputs = new ObjectInputStream[size];
		String root = PathGenerator.getTaskRoot(task);
		PriorityQueue<Record<KEYIN, VALUEIN>> heap = new PriorityQueue<Record<KEYIN, VALUEIN>>(size, new RecordComparator<KEYIN, VALUEIN>());
		for (int i = 0; i < size; i++) {
			inputs[i] = new ObjectInputStream(new FileInputStream(root + "/fetched_partition_" + i));
			Record<KEYIN, VALUEIN> rec = null;
			if ((rec = readRecord(inputs[i], i)) != null)
				heap.add(rec);
		}
		
		FileOutputStream fOut = new FileOutputStream(root + "/merged");
		ObjectOutputStream oOut = new ObjectOutputStream(fOut);
		
		while (!heap.isEmpty()) {
			Record<KEYIN, VALUEIN> rec = heap.poll();
			oOut.writeObject(rec.key);
			oOut.writeObject(rec.value);
			Record<KEYIN, VALUEIN> tmp = null;
			if ((tmp = readRecord(inputs[rec.seqNum], rec.seqNum)) != null)
				heap.add(tmp);
			nRecs++;
		}	
		System.out.println("RECORDS:  " + nRecs);
		
		oOut.close();
		fOut.close();
		return nRecs;
	}
}
