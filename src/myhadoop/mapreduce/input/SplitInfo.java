package myhadoop.mapreduce.input;

import java.io.Serializable;
import java.util.List;

public class SplitInfo implements Serializable{
	private String splitClassName;
	private InputSplit split;
	
	public SplitInfo(InputSplit split) {
		this.split = split;
		this.splitClassName = split.getClass().getName();
	}
	
	public InputSplit getInputSplit() {
		return split;
	}
	
	public String getSplitClassName() {
		return splitClassName;
	}
	
	public List<String> getLocationList() {
		return split.getLocationList();
	}
	
	public String toString() {
		return "[Start: " + split.getStart() + ", Length: " + split.getLength() + "]";
	}
}
