package myhadoop.mapreduce.io;

import java.io.Serializable;

public class Text implements Comparable<Text>, Serializable {
	
	String text;
	
	public Text(String _text) {
		text = _text;
	}
	
	public Text() {
		
	}
	
	public void set(String str) {
		text = str;
	}
	
	public void append(String str) {
		text = text + str;
	}
	
	public String getText() {
		return text;
	}
	
	public String toString() {
		return text;
	}

	public int compareTo(Text o) {
		return text.compareTo(o.getText());
	}
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof Text))
			return false;
		Text t = (Text) o;
		return text.equals(t.getText());
	}
	
	@Override
	public int hashCode() {
		return text.hashCode();
	}

}
