package myhadoop.fs;

import java.io.Serializable;

public class DataNodeRef implements Serializable {
	String host;
	int port;
	
	public DataNodeRef(String _host, int _port) {
		host = _host;
		port = _port;
	}
	
	public boolean equals(Object o) {
		DataNodeRef r2 = (DataNodeRef) o;
		return host.equals(r2.getHost()) && port == r2.getPort();
	}
	
	public String getHost() {
		return host;
	}
	
	public int getPort() {
		return port;
	}
}
