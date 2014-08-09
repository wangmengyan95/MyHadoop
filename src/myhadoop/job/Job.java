package myhadoop.job;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;

import myhadoop.fs.FSDataOutputStream;
import myhadoop.fs.FileStatus;
import myhadoop.fs.FileSystem;
import myhadoop.fs.Path;
import myhadoop.jobtracker.JobTrackerRMI;
import myhadoop.mapreduce.input.InputFormat;
import myhadoop.mapreduce.input.InputSplit;
import myhadoop.mapreduce.input.SplitInfo;
import myhadoop.mapreduce.input.TextInputFormat;
import myhadoop.mapreduce.map.Mapper;
import myhadoop.mapreduce.output.OutputFormat;
import myhadoop.mapreduce.reduce.Reducer;
import myhadoop.utility.Configuration;
import myhadoop.utility.Log;
import myhadoop.utility.PathGenerator;

/*
 * This class is used by clients to submit Job to the 
 * JobTracker.
 */
public class Job {
	private JobContext context;
	private JobTrackerRMI jobTrackerRMI;
	private FileSystem fs;
	
	private Class<?> jobClass;
	private Class<?> mapperClass;
	private Class<?> reducerClass;
	
	boolean reduce;

	public Job(Configuration conf, String name) {
		this.context = new JobContext(conf);
		this.context.setJobName(name);
		
		reduce = true;
	}

	public void submitJob() throws RemoteException, NotBoundException {
		// Get jobTracker stub
		Registry registry = LocateRegistry.getRegistry(context
				.getJobTrackerHost());
		this.jobTrackerRMI = (JobTrackerRMI) registry
				.lookup(JobTrackerRMI.RMIName);

		// Get jobID
		int ID = jobTrackerRMI.getJobID();
		context.setJobID(ID);

//		fs = FileSystem.getFileSystem(context.getFileSystem());
		fs = context.getFileSystemInstance();
		try {
			if (!checkInputPath()) {
				Log.write("Launch job failed: input path [" + context.getInputPath() + "] does not exist.");
				return;
			}
			// Create job dir on fs
			createJobDirOnFs(ID);
			// Submit job file to fs
			submitJobFileToFS();
			// Submit split info to fs
			submitSplitInfoToFs();
			// Submit configuration file to fs
			submitJobConfigurationToFs();
			
			// Submit job to job tracker
			jobTrackerRMI.submitJob(new Path(context.getJobConfigurationFilePath()));
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public boolean checkInputPath() {
		if (context.getInputPath() == null)
			return false;
		FileStatus status = fs.getFileStatus(new Path(context.getInputPath()));
		if (status == null || !status.isDir())
			return false;
		return true;
	}

	public JobContext getContext() {
		return context;
	}

	public void setContext(JobContext context) {
		this.context = context;
	}

	public void setInputFormatClass(Class<? extends InputFormat> inputFormatClass) {
		context.setInputFormatClass(inputFormatClass);
	}
	
	public void setMapperClass(Class<? extends Mapper> mapperClass) {
		this.mapperClass = mapperClass;
		context.setMapperClass(mapperClass);
	}
	
	public void setReducerClass(Class<? extends Reducer> reducerClass) {
		this.reducerClass = reducerClass;
		context.setReducerClass(reducerClass);
	}
	
	public void setInputPath(String path) {
		context.setInputPath(path);
	}
	
	public void setOutputPath(String path) {
		context.setOutputPath(path);
	}
	
	public void setJarClass(Class<?> mainClass) {
		jobClass = mainClass;
		context.setJarClassName(jobClass);
	}
	
	public void setOutputFormatClass(Class<? extends OutputFormat> c) {
		context.setOutputFormatClass(c);
	}
	
	public void setOutputKeyClass(Class<?> c) {
		context.setOutputKeyClass(c);
	}
	
	public void setOutputValueClass(Class<?> c) {
		context.setOutputValueClass(c);
	}
	
	public void setMapOutputKeyClass(Class<?> c) {
		context.setMapOutputKeyClass(c);
	}
	
	public void setMapOutputValueClass(Class<?> c) {
		context.setMapOutputValueClass(c);
	}
	
	public void setNumReduceTasks(int n) {
		if (n <= 0)
			reduce = false;
		context.setReduceTaskNum(n);
	}

	private void createJobDirOnFs(int jobID) throws IOException {
		Path configurationRoot = new Path(context.getConfigurationRoot());
		Path jobDir = new Path(configurationRoot, String.valueOf(jobID));
		fs.mkdir(jobDir);
		// Write job dir path to conf
		context.setJobDir(jobDir.getPath());
	}
	
	private void submitJobFileToFS() throws IOException {
		Path jobDir = new Path(context.getJobDir());
		
		uploadClassFile(jobDir, mapperClass);
		uploadClassFile(jobDir, reducerClass);
		uploadClassFile(jobDir, jobClass);
		context.setJarFilePath(jobDir.getPath());
	} 
	
	private void uploadClassFile(Path jobDir, Class<?> jobClass) throws IOException {
		// Create job configuration file
		Path classFile = new Path(jobDir, getClassFileName(jobClass));
		// Create file output stream
		FSDataOutputStream output = fs.create(classFile);
		InputStream classInput = jobClass.getResourceAsStream(getClassFileName(jobClass));
		byte[] buf = new byte[1024];
		int bytesRead = -1;
		while ((bytesRead = classInput.read(buf)) != -1)
			output.write(buf, 0, bytesRead);
		output.close();
	}
	

	private void submitSplitInfoToFs()  throws InstantiationException, IllegalAccessException, IOException, ClassNotFoundException {
		// Create split
		Class<? extends InputFormat> inputFormatClass = context.getInputFormatClass();
		InputFormat<?, ?> inputFormat = inputFormatClass.newInstance();
		List<InputSplit> inputSplitList = inputFormat.getSplitList(context);
		
		Path jobDir = new Path(context.getJobDir());
		// Create split file
		Path splitFile = new Path(jobDir, "job.split");
		context.setSplitInfoPath(splitFile.getPath());
		// Create file output stream
		FSDataOutputStream output = fs.create(splitFile);
		ObjectOutputStream writter = new ObjectOutputStream(output);
		// Write SplitInfo
		ArrayList<SplitInfo> splitInfoList = new ArrayList<SplitInfo>();
		for (InputSplit inputSplit : inputSplitList) {
			SplitInfo splitInfo = new SplitInfo(inputSplit);
			splitInfoList.add(splitInfo);
		}
		writter.writeObject(splitInfoList);
		writter.close();
		Log.write("Write split info" + splitInfoList.toString());
	}
	
	private void submitJobConfigurationToFs() throws IOException {
		Path jobDir = new Path(context.getJobDir());
		// Create job configuration file
		Path configurationFile = new Path(jobDir, "job.configuration");
		context.setJobConfigurationFilePath(configurationFile.getPath());
		// Create file output stream
		FSDataOutputStream output = fs.create(configurationFile);
		// Write configuration
		context.getConf().writeConfiguration(output);
	}
	
	private String getClassName(Class<?> myClass) {
		String fullName = myClass.getName();
		return fullName;
//		int ind = fullName.lastIndexOf('.');
//		return fullName.substring(ind + 1);
	}
	
	public String getClassFileName(Class<?> myClass) {
		return PathGenerator.getClassFileName(myClass.getName());
	}

	public static void main(String[] args) throws RemoteException,
			NotBoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "job");
		job.setInputFormatClass(TextInputFormat.class);
		job.setInputPath("./input");
		job.submitJob();
	}
}
