package myhadoop.utility;

import java.lang.management.ManagementFactory;

import com.sun.management.OperatingSystemMXBean;

public class HostMonitor {
	private static OperatingSystemMXBean os= (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
	
	public int getAvailableProcessors(){
		return os.getAvailableProcessors();
	}
	
	public double getSystemLoadAverage() {
		return os.getSystemLoadAverage();
	}
	
	public long getFreePhysicalMemorySize() {
		return os.getFreePhysicalMemorySize();
	}
	
	public long getTotalPhysicalMemorySize() {
		return os.getTotalPhysicalMemorySize();
	}
}
