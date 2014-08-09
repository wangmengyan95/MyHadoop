package myhadoop.mapreduce.input;

import java.util.ArrayList;
import java.util.List;

import myhadoop.fs.Path;

public class FileInputSplit extends InputSplit {
	private long length;
	private long start;
	private Path path;
	private List<String> hostList;

	public FileInputSplit(long length, long start, String path,
			List<String> hostList) {
		this.length = length;
		this.start = start;
		this.setPath(new Path(path));
		this.hostList = hostList;
	}

	@Override
	public long getLength() {
		return length;
	}

	@Override
	public List<String> getLocationList() {
		return this.hostList;
	}

	@Override
	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public Path getPath() {
		return path;
	}

	public void setPath(Path path) {
		this.path = path;
	}
}
