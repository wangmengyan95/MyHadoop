package myhadoop.mapreduce.io;

import java.io.Serializable;

public class IntWritable implements Comparable<IntWritable>, Serializable {
	
	Integer value;
	
	public IntWritable(Integer _value) {
		value = _value;
	}
	
	public Integer getValue() {
		return value;
	}

	public int compareTo(IntWritable o) {
		return value.compareTo(o.getValue());
	}
	
	public String toString() {
		return value + "";
	}
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof IntWritable))
			return false;
		IntWritable t = (IntWritable) o;
		return value.equals(t.getValue());
	}
	
	@Override
	public int hashCode() {
		return value.hashCode();
	}
	
}
