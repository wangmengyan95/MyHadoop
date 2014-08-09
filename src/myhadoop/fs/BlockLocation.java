package myhadoop.fs;

import java.io.Serializable;
import java.util.ArrayList;

public class BlockLocation implements Serializable {
	  private ArrayList<String> hostList; //hostnames of datanodes
	  private ArrayList<Integer> portList; //portNumber of datanodes
	  private long offset;  //offset of the of the block in the file
	  private long size;
	  
	  public BlockLocation(ArrayList<String> hostList, ArrayList<Integer> portList, long offset, long size) {
		  this.setHostList(hostList);
		  this.setPortList(portList);
		  this.setOffset(offset);
		  this.setSize(size);
	  }

	public long getOffset() {
		return offset;
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public ArrayList<String> getHostList() {
		return hostList;
	}

	public void setHostList(ArrayList<String> hostList) {
		this.hostList = hostList;
	}

	public ArrayList<Integer> getPortList() {
		return portList;
	}

	public void setPortList(ArrayList<Integer> portList) {
		this.portList = portList;
	}
}
