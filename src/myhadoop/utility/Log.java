package myhadoop.utility;

public class Log {
	public static void write(String str) {
		System.out.println(str);
	}

	public static void warn(String str) {
		System.err.println(str);
	}
}
