package myhadoop.tasktracker;

import java.io.Serializable;
public class TaskTrackerID implements Serializable {
	private int ID;
	private String host;
	private int port;

	public TaskTrackerID(int ID, String host, int port) {
		this.ID = ID;
		this.host = host;
		this.port = port;
	}

	public int getID() {
		return ID;
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("{ID: " + ID + "|");
		builder.append("host: " + host + "|");
		builder.append("port: " + port + "}");
		return builder.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ID;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + port;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TaskTrackerID other = (TaskTrackerID) obj;
		if (ID != other.ID)
			return false;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		if (port != other.port)
			return false;
		return true;
	}
}
