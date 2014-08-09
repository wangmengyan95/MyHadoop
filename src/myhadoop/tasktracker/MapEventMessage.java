package myhadoop.tasktracker;

import java.io.Serializable;

public class MapEventMessage implements Serializable {
	public static final int MESSAGE_FETCH_MAP_OUTPUT = 11;
	public static final int MESSAGE_FETCH_MAP_OUTPUT_ACK = 12;
	public static final int MESSAGE_FETCH_MAP_OUTPUT_FAIL = 19;
	
	public static final int MESSAGE_UNDEFINED = MESSAGE_FETCH_MAP_OUTPUT_FAIL;
	
	int type;
	
	TaskAttemptID attemptID;
	int port;
	int partitionNum;
	
	public MapEventMessage(int _type) {
		type = _type;
	}
	
	public int getType() {
		return type;
	}

	public TaskAttemptID getAttemptID() {
		return attemptID;
	}

	public int getPort() {
		return port;
	}
	
	public int getPartitionNum() {
		return partitionNum;
	}
	
	public MapEventMessage withType(int _type) {
		type = _type;
		return this;
	}

	public MapEventMessage withAttemptID(TaskAttemptID _attemptID) {
		attemptID = _attemptID;
		return this;
	}
	
	public MapEventMessage withPort(int _port) {
		port = _port;
		return this;
	}
	
	public MapEventMessage withPartitionNum(int _partitionNum) {
		partitionNum = _partitionNum;
		return this;
	}
}
