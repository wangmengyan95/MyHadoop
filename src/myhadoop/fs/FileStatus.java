package myhadoop.fs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FileStatus implements Serializable {
	private String path;
	private List<String> hostList;
	private List<Integer> portList;
	private boolean isDir;
	private long size;
	private long blockSize;
	
	private ArrayList<BlockLocation> blockLocations;
	
	public FileStatus(String path, boolean isDir, long size, long blockSize) {
		this.path = path;
		this.isDir = isDir;
		this.size = size;
		this.setBlockSize(blockSize);
	}
	
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public List<String> getHostList() {
		return hostList;
	}
	public void setHostList(List<String> hostList) {
		this.hostList = hostList;
	}
	public List<Integer> getPortList() {
		return portList;
	}
	public void setPortList(List<Integer> portList) {
		this.portList = portList;
	}
	public boolean isDir() {
		return isDir;
	}
	public void setDir(boolean isDir) {
		this.isDir = isDir;
	}
	public long getSize() {
		return size;
	}
	public void setSize(long size) {
		this.size = size;
	}
	public long getBlockSize() {
		return blockSize;
	}
	public void setBlockSize(long blockSize) {
		this.blockSize = blockSize;
	}
	public ArrayList<BlockLocation> getBlockLocations() {
		return blockLocations;
	}
	public void setBlockLocations(ArrayList<BlockLocation> blockLocations) {
		this.blockLocations = blockLocations;
	}
	
	public String toString() {
		return "[FileStatus] path:" + path + "|isDir:" + isDir + "|size:" + size;
	}
	
}
