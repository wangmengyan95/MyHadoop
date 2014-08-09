package myhadoop.utility;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

public class FileOperator {
	
	static final int LINE_BREAK_SIZE = 1;
	
	static final int BUFFER_SIZE = 4096;
	
	public static ArrayList<Integer> splitFile(String path, int size) throws IOException {
		if (size <= 0)
			throw new IOException("Split size can not be less than or equal to zero.");
		
		ArrayList<Integer> ret = new ArrayList<Integer>();
		
		BufferedReader rd = new BufferedReader(new FileReader(path));
		
		String line = null;
		int curSize = 0;
		int lastIndex = 0;
		ret.add(0);
		
		while ((line = rd.readLine()) != null) {
			int toAdd = line.length() + LINE_BREAK_SIZE;
			
			if (curSize + toAdd > size) {
				lastIndex = lastIndex + curSize;
				ret.add(lastIndex);
				curSize = 0;
			}
			
			curSize += toAdd;
		}
		
		ret.add(lastIndex + curSize);
		
		rd.close();
		return ret;
	}
	
	public static long getSize(String path) {
		File f = new File(path);
		if (!f.exists())
			return -1;
		return f.length();
	}
	
	// Send a local file to an outputstream.
	public static void sendFile(OutputStream out, String fileName) throws IOException {
		FileInputStream fileInput = new FileInputStream(new File(fileName));
		byte[] buffer = new byte[BUFFER_SIZE];

		int len = 0;
		while ((len = fileInput.read(buffer)) != -1) {
			out.write(buffer, 0, len);
			out.flush();
		}
		fileInput.close();
	}

	// Receive a file from an inputstream.
	public static void recvFile(InputStream in, String fileName) throws IOException {
		// Create the file if the file does not exist.
		FileOutputStream fileOutput = new FileOutputStream(fileName);
		byte[] buffer = new byte[BUFFER_SIZE];

		int len = 0;
		while ((len = in.read(buffer)) != -1) {
			fileOutput.write(buffer, 0, len);
			fileOutput.flush();
		}
		fileOutput.close();
	}
	
	// Receive a file from an inputstream to buffer.
	public static int recvFileToBuffer(InputStream in, byte[] buffer, long totalLength) throws IOException {
		int len = 0;
		int cur = 0;
		int remain = (int) totalLength;
		
		while (remain > 0 && (len = in.read(buffer, cur, remain)) > 0) {
			remain -= len;
			cur += len;
		}
		
		return cur;
	}

	// Send a file in a buffer to an outputstream.
	public static void dispatchFile(byte[] buffer, OutputStream out, long totalLength) throws IOException {
		out.write(buffer, 0, (int) totalLength);
		out.flush();
	}
	
	// Receive a file with certain length from an inputstream.
	public static void recvFileWithLength(InputStream in, String fileName, long totalLength) throws IOException {
		// Create the file if the file does not exist.
		FileOutputStream fileOutput = new FileOutputStream(fileName);
		byte[] buffer = new byte[BUFFER_SIZE];

		int len = 0;
		int remain = (int) totalLength;
		while (remain > 0
				&& (len = in.read(buffer, 0, Math.min(remain, BUFFER_SIZE))) > 0) {
			fileOutput.write(buffer, 0, len);
			fileOutput.flush();
			remain -= len;
		}
		fileOutput.close();
	}
	
	// Get the parent path of a path.
	public static String getParentPath(String path) {
		if (path.charAt(path.length() - 1) == '/')
			path = path.substring(0, path.length() - 1);

		int lastSlashIndex = path.lastIndexOf('/');
		if (lastSlashIndex == -1)
			return ".";
		else
			return path.substring(0, lastSlashIndex);
	}
	
	// Get the file name of a path.
	public static String getFileName(String path) {
		if (path.charAt(path.length() - 1) == '/')
			path = path.substring(0, path.length() - 1);

		int lastSlashIndex = path.lastIndexOf('/');
		if (lastSlashIndex == -1)
			return path;
		else
			return path.substring(lastSlashIndex + 1);
	}
	
	// Get full name of a filename under a dir.
	public static String getFullName(String dir, String fileName) {
		return removeSlash(dir) + "/" + getFileName(fileName);
	}
	
	// Remove beginning and ending '/'s on a path.
	public static String removeSlash(String str) {
		if (str.length() > 0 && str.charAt(0) == '/')
			str = str.substring(1);
		if (str.length() > 0 && str.charAt(str.length() - 1) == '/')
			str = str.substring(0, str.length() - 1);
		return str;
	}
}
