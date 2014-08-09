package myhadoop.mapreduce.input;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import myhadoop.fs.DistributedFileSystem;
import myhadoop.fs.FSDataInputStream;
import myhadoop.fs.FileSystem;
import myhadoop.mapreduce.io.Text;
import myhadoop.tasktracker.TaskAttemptContext;

public class LineRecordReader extends RecordReader<Long, Text> {
	private long start;
	private long pos;
	private long end;
	private BufferedReader reader;
	private Long key;
	private Text value;
	
	FileSystem fs;

	@Override
	public void initialize(TaskAttemptContext context, InputSplit inputSplit) {
		FileInputSplit fileInputSplit = (FileInputSplit) inputSplit;
		start = fileInputSplit.getStart();
		end = start + fileInputSplit.getLength();
		
		System.out.println("[SPLIT] Start:" + start + " End:" + end);

		// Get filesystem
		FileSystem fs = context.getFileSystemInstance();
		FSDataInputStream input = null;
		try {
			input = fs.open(fileInputSplit.getPath());
			// If start is not zero, then the current start may
			// in the middle of line.
			// In this situation, I move back the inputstream by one byte,
			// then readLine to skip the current line.
			// Currently do not think about encoding problem.
			// Assume one char is one byte (Use ASCII to encode).
			boolean skipFirstLine = false;
			if (start > 0) {
				start--;
				skipFirstLine = true;
			}
			input.seek(start);
			reader = new BufferedReader(new InputStreamReader(input));
			if (skipFirstLine) {
				String firstLine = reader.readLine();
				// start += firstLine.getBytes().length + context.getEOFSize();
				start += firstLine.getBytes().length + 1;
			}
			pos = start;

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		if (key == null) {
			key = new Long(pos);
		}
		if (value == null) {
			value = new Text("");
		}

		String line = "";
		boolean hasFinished = true;
		if (pos < end) {
			line = reader.readLine();
//			// EOF
//			if (line == null) {
//				break;
//			}
			if (line != null) {
				hasFinished = false;
				pos += line.getBytes().length + 1;
			}
		}
		if (hasFinished) {
			key = null;
			value = null;
			return false;
		} else {
			key = pos;
			value = new Text(line);
			if (line.equals("aaa")) {
				System.out.println();
			}
			return true;
		}
	}

	@Override
	public Long getCurrentKey() {
		return key;
	}

	@Override
	public Text getCurrentValue() {
		return value;
	}

	/**
	 * Get the progress within the split
	 */
	@Override
	public double getProgress() {
		if (start == end) {
			return 0.0;
		} else {
			return Math.min(1.0, (pos - start) / (double) (end - start));
		}
	}

	@Override
	public synchronized void close() throws IOException {
		if (reader != null) {
			reader.close();
		}
	}
}
