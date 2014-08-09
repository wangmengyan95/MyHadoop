package myhadoop.utility;

import java.util.Random;

public class PortGenerator {
	
	static int MIN_PORT = 20000;
	static int MAX_PORT = 22000;
	
	public static int generatePort() {
		Random random = new Random();
		return MIN_PORT + random.nextInt() % (MAX_PORT - MIN_PORT);
	}
	
}
