package myhadoop.mapreduce.io;

import java.io.Serializable;

public class DoubleWritable implements Comparable<DoubleWritable>, Serializable {
	
	Double value;
	
	public DoubleWritable(Double _value) {
		value = _value;
	}
	
	public Double getValue() {
		return value;
	}

	public int compareTo(DoubleWritable o) {
		return value.compareTo(o.getValue());
	}
	
	public String toString() {
		return value + "";
	}
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof DoubleWritable))
			return false;
		DoubleWritable t = (DoubleWritable) o;
		return value.equals(t.getValue());
	}
	
	@Override
	public int hashCode() {
		return value.hashCode();
	}
}
