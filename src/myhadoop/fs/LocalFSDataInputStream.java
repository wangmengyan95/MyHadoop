package myhadoop.fs;

import java.io.IOException;
import java.io.RandomAccessFile;

public class LocalFSDataInputStream extends FSDataInputStream{
	private RandomAccessFile file;
	
	public LocalFSDataInputStream(RandomAccessFile file) {
		this.file = file;
	}
	
	@Override
	public void seek(long pos) throws IOException {
		file.seek(pos);
	}

	@Override
	public int read() throws IOException {
		return file.read();
	}
	
	public void close() throws IOException {
		file.close();
	}
}
