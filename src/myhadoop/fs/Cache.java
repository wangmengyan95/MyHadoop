package myhadoop.fs;

import java.io.IOException;

public class Cache {
	static final int CACHE_SIZE = NameNode.BUFFER_SIZE;
	
	String path;
	long blockOffset;
	long length;
	byte[] buffer;
	
	long cursor;
	
	FSClient client;
	
	public Cache(FSClient _client, FileStatus status) {
		path = status.getPath();
		length = status.getSize();
		System.out.println("length: " + length);
		cursor = 0;
		blockOffset = -1;
		buffer = new byte[CACHE_SIZE];
		client = _client;
	}
	
	public void seek(long offset) throws IOException {
		if (offset < 0 || offset >= length)
			throw new IOException("Seek out of bound: " + offset);
		cursor = offset;
	}
	
	public int read() throws IOException {
		updateBuffer(cursor);
		if (cursor == length)
			return -1;
		int cur = (int) (cursor - blockOffset);
		cursor++;
		int base = 0;
		base ^= ((int) buffer[cur]) & 0xFF;
		return base;
	}
	
	/*
	 * Update the buffer of the current cache if the current position exceeds the range of buffer.
	 */
	private void updateBuffer(long offset) throws IOException {
		if (getBlockOffset(offset) == blockOffset)
			return;
		try {
			client.readFileBlockToBuffer(path, offset, buffer);
			blockOffset = getBlockOffset(offset);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private long getBlockOffset(long offset) {
		return (offset / NameNode.MAX_CHUNK_SIZE) * NameNode.MAX_CHUNK_SIZE;
	}
}
