package myhadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DistributedFileSystem extends FileSystem {
	
	FSClient client;
	
	public DistributedFileSystem(String host, int port) {
		client = new FSClient(host, port);
	}

	@Override
	public FileStatus getFileStatus(Path path) {
		try {
			FileStatus ret = client.listThis(path.getPath());
			return ret;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public List<FileStatus> getFileStatusList(Path path) {
		try {
			ArrayList<FileStatus> ret = client.list(path.getPath());
			return ret;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public FSDataInputStream open(Path path) throws FileNotFoundException {
		return new DFSDataInputStream(client, path.getPath());
	}

	@Override
	public void mkdir(Path path) {
		try {
			client.mkdir(path.getPath());
		} catch (Exception e) {
			e.printStackTrace();
		}	
	}

	@Override
	public FSDataOutputStream create(Path path) throws IOException {
		return new DFSDataOutputStream(client, path.getPath());
	}

	@Override
	public ArrayList<BlockLocation> getBlockLocationList(FileStatus fileStatus) {
		return fileStatus.getBlockLocations();
	}

	@Override
	public void uploadFile(Path targetPath, Path sourcePath) throws IOException, ClassNotFoundException {
		client.uploadFile(targetPath.getPath(), sourcePath.getPath());
	}

}
