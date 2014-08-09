package myhadoop.mapreduce.input;

import java.util.ArrayList;
import java.util.List;

import myhadoop.fs.BlockLocation;
import myhadoop.fs.FileStatus;
import myhadoop.fs.FileSystem;
import myhadoop.fs.Path;
import myhadoop.job.JobContext;

public abstract class FileInputFormat<K, V> extends InputFormat<K, V> {

	private FileSystem fs;

	@Override
	public List<InputSplit> getSplitList(JobContext context) {
		// Initialize file system if necessary
		if (fs == null) {
			this.fs = context.getFileSystemInstance();
		}

		// Get splitSize limitation
		long minSize = context.getSplitMinSize(); // MB in configuration
		long maxSize = context.getSplitMaxSize(); // MB in configuration

		List<InputSplit> splitList = new ArrayList<InputSplit>();
		List<FileStatus> fileStatusList = listStatus(context);
		// Split each files
		for (FileStatus fileStatus : fileStatusList) {
			System.out.println("Splitting file: " + fileStatus.getPath() + " Length: " + fileStatus.getSize());
			long remainingFileSize = fileStatus.getSize();
			long offset = 0;
			// Compute final split size
			long splitSize = computeSplitSize(minSize, maxSize, fileStatus.getBlockSize());
			// Get block location of this file
			ArrayList<BlockLocation> blockLocationList = fs.getBlockLocationList(fileStatus);
			
			int sum = 0;
			for (BlockLocation l : blockLocationList)
				sum += l.getSize();
			
			while (remainingFileSize > 0) {
				long size = remainingFileSize >= splitSize ? splitSize : remainingFileSize;
				// Compute block index to get host and port
				int blockIndex = computeBlockIndex(blockLocationList, offset);
				FileInputSplit split = new FileInputSplit(size, offset, fileStatus.getPath(), blockLocationList.get(
						blockIndex).getHostList());
				remainingFileSize -= size;
				offset += size;
				splitList.add(split);
			}
		}
		return splitList;
	}

	@Override
	public RecordReader<K, V> createRecordReader(InputSplit split) {
		return null;
	}

	// Get file status from DFS based on the input file path.
	protected List<FileStatus> listStatus(JobContext context) {
		List<FileStatus> fileStatusList = new ArrayList<FileStatus>();
		String[] paths = getPaths(context.getInputPath());

		for (String path : paths) {
			try {
				FileStatus fileStatus = fs.getFileStatus(new Path(path));
				// If path is a directory, get all non dir files under it.
				if (fileStatus.isDir()) {
					for (FileStatus subFileStatus : fs.getFileStatusList(new Path(fileStatus.getPath()))) {
						if (!subFileStatus.isDir()) {
							fileStatusList.add(subFileStatus);
						}
					}
				}
				// If path is a directory, add it to list directly
				else {
					fileStatusList.add(fileStatus);
				}
			} catch (Exception e) {
				// File not existed on DFS exception
			}
		}
		return fileStatusList;
	}

	/**
	 * Get input paths from raw input path String. Multiple input paths are
	 * separated by comma.
	 */
	protected String[] getPaths(String rawPath) {
		return rawPath.split(",");
	}

	protected long computeSplitSize(long minSize, long maxSize, long blockSize) {
		return Math.max(minSize, Math.min(maxSize, blockSize));
	}

	// TODO
	// When the maxSize can not be divided by the blockSize, a split may cross
	// more block.
	// Currently, I do not think of this problem.
	protected int computeBlockIndex(ArrayList<BlockLocation> blockLocationList, long offset) {
		for (int i = 0; i <= blockLocationList.size() - 1; i++) {
			BlockLocation blockLocation = blockLocationList.get(i);
			boolean inBlock = blockLocation.getOffset() <= offset
					&& offset < blockLocation.getOffset() + blockLocation.getSize();
			if (inBlock) {
				return i;
			}
		}
		throw new IllegalArgumentException("Input offset is beyond the file");
	}
}
