package myhadoop.job;

import myhadoop.fs.DistributedFileSystem;
import myhadoop.fs.FileSystem;
import myhadoop.mapreduce.input.InputFormat;
import myhadoop.utility.Configuration;
import myhadoop.utility.Context;

/**
 * This class is a wrapper of configuration file. It provides some useful
 * function to get job related configuration.
 */
public class JobContext extends Context {
	public JobContext(Configuration conf) {
		super(conf);
	}

	public int getJobID() {
		return conf.getInt("JobID");
	}

	public void setJobID(int jobID) {
		conf.add("JobID", String.valueOf(jobID));
	}

	public String getJobName() {
		return conf.getString("JobName");
	}

	public void setJobName(String jobName) {
		conf.add("JobName", jobName);
	}

	public String getJobTrackerHost() {
		return conf.getString("JobTrackerHost");
	}

	public int getJobTrackerPort() {
		return conf.getInt("JobTrackerPort");
	}

	public void setInputPath(String path) {
		conf.add("RawInputPath", path);
	}

	public String getInputPath() {
		return conf.getString("RawInputPath");
	}
	
	public void setOutputPath(String path) {
		conf.add("RawOutputPath", path);
	}
	
	public String getOutputPath() {
		return conf.getString("RawOutputPath");
	}

	public long getSplitMinSize() {
		return conf.getLong("SplitMinSize") * 1000000;
	}

	public long getSplitMaxSize() {
		return conf.getLong("SplitMaxSize") * 1000000;
	}

	public String getJobTrackerRoot() {
		return conf.getString("JobTrackerRoot");
	}

	public void setJobDir(String path) {
		conf.add("JobDir", path);
	}

	public String getJobDir() {
		return conf.getString("JobDir");
	}

	public void setInputFormatClass(Class<? extends InputFormat> inputFormatClass) {
		conf.add("InputFormatClass", inputFormatClass.getName());
	}

	public Class<? extends InputFormat> getInputFormatClass() throws ClassNotFoundException {
		String className = conf.getString("InputFormatClass");
		return (Class<? extends InputFormat>) Class.forName(className);
	}

	public void setSplitInfoPath(String path) {
		conf.add("SplitInfoPath", path);
	}

	public String getSplitInfoPath() {
		return conf.getString("SplitInfoPath");
	}

	public void setJobConfigurationFilePath(String path) {
		conf.add("JobConfigurationFile", path);
	}

	public String getJobConfigurationFilePath() {
		return conf.getString("JobConfigurationFile");
	}

	public int getMaxMapTaskNumPerJob() {
		return conf.getInt("MaxMapTaskNumPerJob");
	}

	public void setMapTaskNum(int num) {
		conf.add("MapTaskNum", String.valueOf(num));
	}

	public int getMapTaskNum() {
		return conf.getInt("MapTaskNum");
	}

	public int getMaxReduceTaskNumPerJob() {
		return conf.getInt("MaxReduceTaskNumPerJob");
	}

	public void setReduceTaskNum(int num) {
		conf.add("ReduceTaskNum", num);
	}

	public int getReduceTaskNum() {
		return conf.getInt("ReduceTaskNum");
	}

	public String getConfigurationRoot() {
		return conf.getString("ConfigurationRoot");
	}
	
	public void setJarClassName(Class<?> jarClass) {
		conf.add("JarClass", getClassName(jarClass));
	}

	public void setJarFilePath(String path) {
		conf.add("JarFilePath", path);
	}

	public String getJarFilePath() {
		return conf.getString("JarFilePath");
	}
	
	public void setOutputKeyClass(Class<?> keyClass) {
		conf.add("OutputKeyClass", keyClass.getName());
	}
	
	public Class<?> getOutputKeyClass() throws ClassNotFoundException {
		return Class.forName(conf.getString("OutputKeyClass"));
	}
	
	public void setOutputValueClass(Class<?> valueClass) {
		conf.add("OutputValueClass", valueClass.getName());
	}
	
	public Class<?> getOutputValueClass() throws ClassNotFoundException {
		return Class.forName(conf.getString("OutputValueClass"));
	}
	
	public void setMapOutputKeyClass(Class<?> keyClass) {
		conf.add("MapOutputKeyClass", keyClass.getName());
	}
	
	public void setMapOutputValueClass(Class<?> valueClass) {
		conf.add("MapOutputValueClass", valueClass.getName());
	}
	
	public void setMapperClass(Class<?> mapperClass) {
		conf.add("MapperClass", getClassName(mapperClass));
	}
	
	public Class<?> getMapperClass() throws ClassNotFoundException {
		return Class.forName(conf.getString("MapperClass"));
	}
	
	public void setReducerClass(Class<?> reducerClass) {
		conf.add("ReducerClass", getClassName(reducerClass));
	}
	
	public Class<?> getReducerClass() throws ClassNotFoundException {
		return Class.forName(conf.getString("ReducerClass"));
	}
	
	public void setOutputFormatClass(Class<?> outFormatClass) {
		conf.add("OutputFormatClass", outFormatClass.getName());
	}

	public Class<?> getOutputFormatClass() throws ClassNotFoundException {
		String className = conf.getString("OutputFormatClass");
		return (Class<?>) Class.forName(className);
	}
	
	public String getMapperClassName() {
		return conf.getString("MapperClass");
	}

	public String getReducerClassName() {
		return conf.getString("ReducerClass");
	}
	
	public String getInputFormatClassName() {
		return conf.getString("InputFormatClass");
	}
	
	public String getOutputFormatClassName() {
		return conf.getString("OutputFormatClass");
	}
	
	public String getOutputKeyClassName() {
		return conf.getString("OutputKeyClass");
	}
	
	public String getOutputValueClassName() {
		return conf.getString("OutputValueClass");
	}
	
	public String getMapOutputKeyClassName() {
		return conf.getString("MapOutputKeyClass");
	}
	
	public String getMapOutputValueClassName() {
		return conf.getString("MapOutputValueClass");
	}
	
	public String getJarClassName() {
		return conf.getString("JarClass");
	}
	
	private String getClassName(Class<?> myClass) {
		String fullName = myClass.getName();
		return fullName;
	}
}
