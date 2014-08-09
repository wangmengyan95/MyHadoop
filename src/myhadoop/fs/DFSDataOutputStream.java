package myhadoop.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import myhadoop.utility.FileOperator;

public class DFSDataOutputStream extends FSDataOutputStream {
	
	FSClient client;
	String path;
	FileOutputStream fs;
	String tempFileName;
	
	public DFSDataOutputStream(FSClient _client, String _path) throws IOException {
		client = _client;
		path = _path;
		
		try {
			client.mkdir(FileOperator.getParentPath(path));
			
			tempFileName = generateTempFileName();
			fs = new FileOutputStream(new File(tempFileName));
		} catch (Exception e) {
			throw new IOException(e.getMessage());
		}
		
	}

	@Override
	public void write(int b) throws IOException {
		fs.write(b);
	}
	
	@Override
	public void flush() throws IOException {
		fs.flush();
	}
	
	@Override
	public void close() throws IOException {
		fs.close();
		try {
			client.uploadFile(path, tempFileName);
			
			// Delete the temporary file.
			File file = new File(tempFileName);
			file.delete();
		} catch (Exception e) {
			throw new IOException(e.getMessage());
		}
	}
	
	private String generateTempFileName() {
		Random random = new Random();
		return FileOperator.getFileName(path) + "_temp_" + Math.abs(random.nextInt()) % 1000;
	}

}
