package myhadoop.utility;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import myhadoop.fs.FSDataInputStream;
import myhadoop.fs.FSDataOutputStream;
import myhadoop.fs.FileSystem;
import myhadoop.fs.LocalFileSystem;
import myhadoop.fs.Path;

public class Configuration implements Serializable {
	private Map<String, Object> confMap;

	public Configuration() {
		this.confMap = new HashMap<String, Object>();
		// Default local file system
		FileSystem fs = new LocalFileSystem();

		// Default configuration
		addResource(fs, new Path("./conf"));
	}

	/**
	 * This function load configuration file from a given file system
	 * 
	 * @param fs
	 *            Filesystem
	 * @param path
	 *            The path of the configuration file
	 */
	public Configuration(FileSystem fs, Path path) {
		this.confMap = new HashMap<String, Object>();
		addResource(fs, path);
	}

	/**
	 * This function read configuration from a configuration file by given path.
	 * Then add this to the confMap You can call this method multiple times to
	 * load configuration from multiple files.
	 * 
	 * @param fs
	 * @param path
	 */
	public void addResource(FileSystem fs, Path path) {
		try {
			// Create file input stream
			FSDataInputStream confInput = fs.open(path);
			// Load properties
			Properties properties = new Properties();
			properties.load(confInput);
			// Add to map
			Enumeration<?> e = properties.propertyNames();
			while (e.hasMoreElements()) {
				String key = (String) e.nextElement();
				String value = properties.getProperty(key);
				confMap.put(key, value);
			}
			// Close
			confInput.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return;
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}

	public Object get(String name) {
		if (confMap.containsKey(name)) {
			return confMap.get(name);
		} else {
			throw new IllegalArgumentException("Item does not exist in the configuration.");
		}
	}

	public String getString(String name) {
		return (String) get(name);
	}

	public long getLong(String name) {
		return Long.parseLong((String) get(name));
	}

	public int getInt(String name) {
		return Integer.parseInt((String) get(name));
	}

	public void add(String key, Object value) {
		confMap.put(key, value);
	}

	/**
	 * Write the configuration file to a given output stream
	 * @param output
	 * @throws IOException
	 */
	public void writeConfiguration(FSDataOutputStream output) throws IOException {
		BufferedWriter writter = new BufferedWriter(new OutputStreamWriter(output));
		for (String key : confMap.keySet()) {
			String line = key + "=" + confMap.get(key).toString();
			writter.write(line);
			writter.newLine();
		}
		writter.close();
		return;
	}

	public static void main(String[] args) {
		Configuration conf = new Configuration();
	}
}
