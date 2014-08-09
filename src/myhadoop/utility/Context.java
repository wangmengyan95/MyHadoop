package myhadoop.utility;

import myhadoop.fs.DistributedFileSystem;
import myhadoop.fs.FileSystem;

/**
 * Father class of other context class
 *
 */
public class Context {
	protected Configuration conf;
	
	public Context(Configuration conf) {
		this.conf = conf;
	}
	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}
	
	public String getFileSystem() {
		return conf.getString("FileSystem");
	}
	
	public FileSystem getFileSystemInstance() {
		if (conf.getString("FileSystem") != null) {
			if (conf.getString("FileSystem").equalsIgnoreCase("local"))
				return FileSystem.getFileSystem("local");
			else if (conf.getString("FileSystem").equalsIgnoreCase("Distributed")) {
				String host = conf.getString("DFSHost");
				Integer port = conf.getInt("DFSPort");
				if (host != null && port != null)
					return new DistributedFileSystem(host, port);
			}
		}
		return null;
	}
}
