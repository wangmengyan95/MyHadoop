package myhadoop.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import myhadoop.utility.FileOperator;
import myhadoop.utility.Log;

/*
 * This is the file system abstraction used by the system.
 */
public abstract class FileSystem {
	
	/**
	 *  Get file status by path
	 */
	public abstract FileStatus getFileStatus(Path path);

	/**
	 * List all files under a dir
	 */
	public abstract List<FileStatus> getFileStatusList(Path path);

	/**
	 *  Open a file and return the input stream of this file
	 */
	public abstract FSDataInputStream open(Path path)
			throws FileNotFoundException;
	
	/**
	 *  Make a dir on the file system
	 */	
	public abstract void mkdir(Path path);

	/**
	 * Create a file on the file system and return the output stream of this file
	 */
	public abstract FSDataOutputStream create(Path path) throws IOException;
	
	/**
	 * Return an list containing hostnames, offset and size of portions of the
	 * given file. For a nonexistent file, null will be returned.
	 * 
	 * This call is most helpful with DFS, where it returns hostnames of
	 * machines that contain the given file.
	 * 
	 * For local file system, the FileSystem will simply return an elt containing 'localhost'.
	 */
	public abstract ArrayList<BlockLocation> getBlockLocationList(FileStatus fileStatus);
	
	/**
	 * Upload a file from the local file system to current system.
	 */
	public abstract void uploadFile(Path targetPath, Path sourcePath) throws IOException, ClassNotFoundException;
	
	/**
	 * Copy a file from the current system to local file system. 
	 */
	public void copyToLocalFS(Path path, Path localPath) throws IOException {
		Log.write("[COPY] FS:" + path.getPath() + " to Local:" + localPath.getPath());
		FSDataInputStream input = open(path);
		
		FileSystem local = FileSystem.getFileSystem("local");
		local.mkdir(new Path(FileOperator.getParentPath(localPath.getPath())));
		File file = new File(localPath.getPath());
		OutputStream output = new FileOutputStream(file);

		byte[] buf = new byte[1024];
		int bytesRead = -1;
		while ((bytesRead = input.read(buf, 0, 1000)) != -1)
			output.write(buf, 0, bytesRead);
		output.close();
		input.close();
	}
	
	/**
	 * Copy files under a directory to a folder in local file system.
	 */
	public void copyDirToLocalFS(Path path, Path localPath) throws IOException {
		List<FileStatus> files = getFileStatusList(path);
		FileSystem local = FileSystem.getFileSystem("local");
		local.mkdir(localPath);
		
		for (FileStatus f : files) {
			if (!f.isDir()) {
				copyToLocalFS(new Path(f.getPath()), new Path(FileOperator.getFullName(localPath.getPath(), f.getPath())));
			}
		}
	}
	
	/**
	 * Copy files under a folder in local file system to a folder in certain FS.
	 */
	public void copyLocalDirToFS(Path localPath, Path path) throws IOException, ClassNotFoundException {
		FileSystem local = FileSystem.getFileSystem("local");
		List<FileStatus> files = local.getFileStatusList(localPath);
		System.out.println(files);
		
		for (FileStatus f : files) {
			if (!f.isDir()) {
				uploadFile(new Path(FileOperator.getFullName(path.getPath(), f.getPath())), new Path(f.getPath()));
			}
		}
	}

	/**
	 *  Generate filesystem class based on type
	 */
	public static FileSystem getFileSystem(String type) {
		if (type.equals("local")) {
			return new LocalFileSystem();
		} else {
			return null;
		}
	}
}
