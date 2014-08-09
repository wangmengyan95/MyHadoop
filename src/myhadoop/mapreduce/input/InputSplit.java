package myhadoop.mapreduce.input;

import java.io.Serializable;
import java.util.List;

import myhadoop.fs.Path;

public abstract class InputSplit implements Serializable{
	public abstract long getStart();
	public abstract long getLength();
	public abstract List<String> getLocationList();
}
