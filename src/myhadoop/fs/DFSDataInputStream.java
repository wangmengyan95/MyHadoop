package myhadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;

public class DFSDataInputStream extends FSDataInputStream {
	
	String path;
	FSClient client;
	
	Cache cache;
	
	public DFSDataInputStream(FSClient _client, String _path) throws FileNotFoundException {
		client = _client;
		path = _path;
		
		try {
			FileStatus status = client.listThis(path);
			if (status == null || status.isDir())
				throw new FileNotFoundException("Can not find file " + path + " on DFS.");
			cache = new Cache(client, status);
		} catch (Exception e) {
			throw new FileNotFoundException(e.getMessage());
		}
	}

	@Override
	public void seek(long pos) throws IOException {
		cache.seek(pos);
	}

	@Override
	public int read() throws IOException {
		return cache.read();
	}

}
