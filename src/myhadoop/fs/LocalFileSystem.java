package myhadoop.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class LocalFileSystem extends FileSystem{

	@Override
	public FileStatus getFileStatus(Path path) {
		File file = new File(path.getPath());
		FileStatus fileStatus = new FileStatus(path.getPath(), file.isDirectory(), file.length(), 1);
		return fileStatus;
	}

	@Override
	public List<FileStatus> getFileStatusList(Path path) {
		File dir = new File(path.getPath());
		// Not a dir, return null
		System.out.println(path.getPath());
		if (!dir.isDirectory()) {
			return null;
		}
		// If is a dir, then get all files under the dir
		List<FileStatus> fileStatusList = new ArrayList<FileStatus>();
		for (File file : dir.listFiles()) {
			if (!file.isDirectory() && !file.isHidden()) {
				fileStatusList.add(getFileStatus(new Path(file.getPath())));
			}
		}
		return fileStatusList;
	}

	@Override
	public FSDataInputStream open(Path path) throws FileNotFoundException{
		File file = new File(path.getPath());
		// If is a dir, return null
		if (file.isDirectory()) {
			return null;
		}
		FSDataInputStream input = null;
		input = new LocalFSDataInputStream(new RandomAccessFile(file, "r"));
		return input;
	}

	@Override
	public ArrayList<BlockLocation> getBlockLocationList(FileStatus fileStatus) {
		if (fileStatus == null) {
			return null;
		}
		ArrayList<BlockLocation> blockLocationList = new ArrayList<BlockLocation>();
		ArrayList<String> hostList = new ArrayList<String>();
		hostList.add("localhost");
		ArrayList<Integer> portList = new ArrayList<Integer>();
		portList.add(1234);
		BlockLocation blockLocation = new BlockLocation(hostList, portList, 0, fileStatus.getSize());
		blockLocationList.add(blockLocation);
		return blockLocationList;
	}

	@Override
	public void mkdir(Path path){
		File file = new File(path.getPath());
		file.mkdirs();
	}

	@Override
	public FSDataOutputStream create(Path path) throws IOException {
		File file = new File(path.getPath());
		file.createNewFile();
		return new LocalFSDataOutputStream(file);
	}

	@Override
	public void uploadFile(Path targetPath, Path sourcePath) throws IOException {
		File sourceFile = new File(sourcePath.getPath());
		File targetFile = new File(targetPath.getPath());
		Files.copy(sourceFile.toPath(), targetFile.toPath());
	}
}
