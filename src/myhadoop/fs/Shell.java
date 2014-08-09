package myhadoop.fs;

import java.io.IOException;
import java.util.List;

public class Shell {
	public static void main(String[] args) throws Exception {
		DistributedFileSystem dfs = new DistributedFileSystem("localhost", 2014);
		
		if (args.length == 2 && args[0].equals("mkdir")) {
			dfs.mkdir(new Path(args[1]));
		} else if (args.length == 2 && args[0].equals("list")) {
			List<FileStatus> list = dfs.getFileStatusList(new Path(args[1]));
			if (list != null) {
				System.out.println("TYPE\t" + "SIZE\t" + "PATH");
				for (FileStatus s : list)
					System.out.println((s.isDir() ? "DIR\t     \t" : "FILE\t" + s.getSize() + "\t") + s.getPath());
			}
		} else if (args.length == 3 && args[0].equals("upload")) {
			uploadFile(dfs, args[1], args[2]);
		} else if (args.length == 3 && args[0].equals("get")) {
			downloadFile(dfs, args[1], args[2]);
		} else if (args.length == 3 && args[0].equals("uploaddir")) {
			uploadDir(dfs, args[1], args[2]);
		} else if (args.length == 3 && args[0].equals("getdir")) {
			downloadDir(dfs, args[1], args[2]);
		} else {
			System.out.println("Usage: ");
			System.out.println("	mkdir <DFSDirPath>");
			System.out.println("	list <DFSDirPath>");
			System.out.println("	upload <localFilePath> <DFSFilePath>");
			System.out.println("	get <DFSFilePath> <localFilePath>");
			System.out.println("	uploaddir <localDirPath> <DFSDirPath>");
			System.out.println("	getdir <DFSDirPath> <localDirPath>");
		}
	}
	
	public static void uploadFile(FileSystem fs, String local, String remote) throws ClassNotFoundException, IOException {
		fs.uploadFile(new Path(remote), new Path(local));
	}
	
	public static void downloadFile(FileSystem fs, String remote, String local) throws IOException {
		fs.copyToLocalFS(new Path(remote), new Path(local));
	}
	
	public static void uploadDir(FileSystem fs, String local, String remote) throws ClassNotFoundException, IOException {
		fs.copyLocalDirToFS(new Path(local), new Path(remote));
	}
	
	public static void downloadDir(FileSystem fs, String remote, String local) throws IOException {
		fs.copyDirToLocalFS(new Path(remote), new Path(local));
	}
}
