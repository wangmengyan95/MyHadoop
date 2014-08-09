package myhadoop.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class LocalFSDataOutputStream extends FSDataOutputStream {
	private FileOutputStream output;

	public LocalFSDataOutputStream(File file) throws FileNotFoundException {
		if (file.exists() && !file.isDirectory()) {
			this.output = new FileOutputStream(file);
		} else {
			throw new FileNotFoundException(file.getPath()
					+ "is not a writable file");
		}
	}

	@Override
	public void write(int b) throws IOException {
		output.write(b);
	}

}
