package myhadoop.fs;

import java.io.Serializable;

public class BlockInfo implements Serializable {
	int blockID;
	String host;
	int port;
	long length;
	
	public BlockInfo(int _blockID, String _host, int _port, long _length) {
		blockID = _blockID;
		host = _host;
		port = _port;
		length = _length;
	}

	public int getBlockID() {
		return blockID;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}
	
	public long getLength() {
		return length;
	}
	
}
