package myhadoop.fs;

import java.io.IOException;
import java.io.InputStream;

public abstract class FSDataInputStream extends InputStream{
	public abstract void seek(long pos) throws IOException;
}
