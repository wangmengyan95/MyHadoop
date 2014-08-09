package myhadoop.fs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.ArrayList;

public class FSMessage implements Serializable {
	
	public static final int MESSAGE_SPREAD_REQ = 10;
	public static final int MESSAGE_SPREAD_REQ_ACK = 11;
	public static final int MESSAGE_SPREAD_FAIL = 19;
	
	public static final int MESSAGE_GETBLOCK_REQ = 20;
	public static final int MESSAGE_GETBLOCK_REQ_ACK = 21;
	public static final int MESSAGE_GETBLOCK_FAIL = 29;
	
	public static final int MESSAGE_BLOCKLOC_REQ = 30;
	public static final int MESSAGE_BLOCKLOC_REQ_ACK = 31;
	public static final int MESSAGE_BLOCKLOC_FAIL = 39;
	
	public static final int MESSAGE_MKDIR_REQ = 40;
	public static final int MESSAGE_MKDIR_REQ_ACK = 41;
	public static final int MESSAGE_MKDIR_FAIL = 49;
	
	public static final int MESSAGE_LIST_REQ = 50;
	public static final int MESSAGE_LIST_REQ_ACK = 51;
	public static final int MESSAGE_LIST_FAIL = 59;
	
	public static final int MESSAGE_UPLOAD_REQ = 60;
	public static final int MESSAGE_UPLOAD_REQ_ACK = 61;
	public static final int MESSAGE_UPLOAD_FAIL = 69;
	
	public static final int MESSAGE_DATANODE_REQ = 70;
	public static final int MESSAGE_DATANODE_REQ_ACK = 71;
	public static final int MESSAGE_DATANODE_HEARTBEAT = 72;
	public static final int MESSAGE_DATANODE_REQ_FAIL = 79;
	
	static String[] type_prefix = {"SPREAD", "GETBLOCK", "BLOCKLOC", "MKDIR", "LIST", "UPLOAD", "DATANODE"};
	static String[] type_suffix = {"REQ", "ACK", "HEARTBEAT", "FAIL"};
	
	int type;
	
	int blockID;
	int port;
	String path;
	long length;
	long offset;
	long position;
	int nodeID;
	String nodeHost;
	
	BlockInfo blockInfo;
	
	FileStatus fileStatus;
	ArrayList<FileStatus> fileStatusList;
	
	public FSMessage() {
		
	}
	
	public FSMessage(int _type) {
		type = _type;
	}
	
	public FSMessage withType(int _type) {
		type = _type;
		return this;
	}
	
	public FSMessage withBlockID(int _blockID) {
		blockID = _blockID;
		return this;
	}
	
	public FSMessage withPort(int _port) {
		port = _port;
		return this;
	}
	
	public FSMessage withPath(String _path) {
		path = _path;
		return this;
	}
	
	public FSMessage withLength(long _length) {
		length = _length;
		return this;
	}
	
	public FSMessage withOffset(long _offset) {
		offset = _offset;
		return this;
	}
	
	public FSMessage withPosition(long _position) {
		position = _position;
		return this;
	}
	
	public FSMessage withNodeID(int _nodeID) {
		nodeID = _nodeID;
		return this;
	}
	
	public FSMessage withBlockInfo(BlockInfo _blockInfo) {
		blockInfo = _blockInfo;
		return this;
	}
	
	public FSMessage withFileStatus(FileStatus _fileStatus) {
		fileStatus = _fileStatus;
		return this;
	}
	
	public FSMessage withFileStatusList(ArrayList<FileStatus> _fileStatusList) {
		fileStatusList = _fileStatusList;
		return this;
	}
	
	public FSMessage withNodeHost(String _nodeHost) {
		nodeHost = _nodeHost;
		return this;
	}
	
	public int getType() {
		return type;
	}
	
	public int getBlockID() {
		return blockID;
	}
	
	public int getPort() {
		return port;
	}
	
	public String getPath() {
		return path;
	}
	
	public long getLength() {
		return length;
	}
	
	public long getOffset() {
		return offset;
	}

	public long getPosition() {
		return position;
	}
	
	public int getNodeID() {
		return nodeID;
	}

	public BlockInfo getBlockInfo() {
		return blockInfo;
	}
	
	public FileStatus getFileStatus() {
		return fileStatus;
	}
	
	public ArrayList<FileStatus> getFileStatusList() {
		return fileStatusList;
	}
	
	public String getNodeHost() {
		return nodeHost;
	}
	
	public static FSMessage exchangeMessage(Socket socket, FSMessage sendMessage) throws IOException, ClassNotFoundException {
		ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
		output.flush();
		ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
		output.writeObject(sendMessage);
		return readMessage(input);
	}
	
	public static FSMessage readMessage(ObjectInputStream input) throws ClassNotFoundException, IOException {
		FSMessage message = (FSMessage) input.readObject();
		if (message.getType() != MESSAGE_DATANODE_HEARTBEAT)
			System.out.println(message);
		return message;
	}
	
	public String typeStr() {
		StringBuilder sb = new StringBuilder();
		sb.append(type_prefix[type / 10 - 1]);
		sb.append("_");
		int suffix_i = type % 10 == 9 ? 3 : (type % 10);
		sb.append(type_suffix[suffix_i]);
		return sb.toString();
	}
	
	public String toString() {
		return "[MSG] Type:" + typeStr() + "|blockID:" + blockID + "|port:" + port + "|path:" + path + "|len:" + length + "|pos:" + position;
	}
	
}
