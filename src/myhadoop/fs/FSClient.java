package myhadoop.fs;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;

import myhadoop.utility.FileOperator;
import myhadoop.utility.Log;

/**
 * Performs basic operations for DFS. Wrapped in class DistributedFileSystem.
 */
public class FSClient {
	String nameNodeHost;
	int nameNodePort;
	
	public FSClient(String _nameNodeHost, int _nameNodePort) {
		nameNodeHost = _nameNodeHost;
		nameNodePort = _nameNodePort;
		
		Log.write("FSClient launched: <" + nameNodeHost + ", " + nameNodePort + ">");
	}
	
	public boolean mkdir(String path) throws UnknownHostException, IOException, ClassNotFoundException {
		FSMessage message = new FSMessage(FSMessage.MESSAGE_MKDIR_REQ).withPath(path);
		Socket socket = new Socket(nameNodeHost, nameNodePort);
		FSMessage recv = FSMessage.exchangeMessage(socket, message);
		socket.close();
		
		if (recv.getType() == FSMessage.MESSAGE_MKDIR_REQ_ACK)
			return true;
		else
			return false;
	}
	
	public void uploadFile(String path, String localFile) throws IOException, ClassNotFoundException {
		mkdir(FileOperator.getParentPath(path));
		// Get port information.
		long length = FileOperator.getSize(localFile);
		FSMessage message = new FSMessage(FSMessage.MESSAGE_UPLOAD_REQ).withLength(length).withPath(path);
		Socket socket = new Socket(nameNodeHost, nameNodePort);
		FSMessage recv = FSMessage.exchangeMessage(socket, message);
		
		if (recv.getType() == FSMessage.MESSAGE_UPLOAD_REQ_ACK) {
			FileOperator.sendFile(socket.getOutputStream(), localFile);
		} else if (recv.getType() == FSMessage.MESSAGE_UPLOAD_FAIL) {
			Log.write("Upload failed.");
		}
		socket.close();
	}
	
	public String getFileBlock(String path, long pos) throws IOException, ClassNotFoundException {
		// Get port information.
		FSMessage message = new FSMessage(FSMessage.MESSAGE_BLOCKLOC_REQ).withPath(path).withPosition(pos);
		Socket socket = new Socket(nameNodeHost, nameNodePort);
		FSMessage recv = FSMessage.exchangeMessage(socket, message);
		
		if (recv.getType() == FSMessage.MESSAGE_BLOCKLOC_REQ_ACK) {
			FSMessage message2 = new FSMessage(FSMessage.MESSAGE_GETBLOCK_REQ).withBlockID(recv.getBlockInfo().getBlockID());
			Socket socket2 = new Socket(recv.getBlockInfo().getHost(), recv.getBlockInfo().getPort());
			FSMessage recv2 = FSMessage.exchangeMessage(socket2, message2);
			
			if (recv2.getType() == FSMessage.MESSAGE_GETBLOCK_REQ_ACK) {
				String tempFileName = generateTempFileName(recv.getBlockInfo().getBlockID());
				FileOperator.recvFileWithLength(socket2.getInputStream(), tempFileName, recv.getBlockInfo().getLength());
				socket2.close();
				return tempFileName;
			} else {
				Log.write("Got the block location, but failed to fetch block from datanode.");
				socket2.close();
			}
		} else if (recv.getType() == FSMessage.MESSAGE_BLOCKLOC_FAIL) {
			Log.write("Get block failed.");
		}
		socket.close();
		return null;
	}
	
	public void readFileBlockToBuffer(String path, long pos, byte[] buffer) throws IOException, ClassNotFoundException {
		if (buffer.length < NameNode.MAX_CHUNK_SIZE)
			throw new IOException("Buffer is too small.");
		
		FSMessage message = new FSMessage(FSMessage.MESSAGE_BLOCKLOC_REQ).withPath(path).withPosition(pos);
		Socket socket = new Socket(nameNodeHost, nameNodePort);
		FSMessage recv = FSMessage.exchangeMessage(socket, message);
		if (recv.getType() == FSMessage.MESSAGE_BLOCKLOC_REQ_ACK) {
			FSMessage message2 = new FSMessage(FSMessage.MESSAGE_GETBLOCK_REQ).withBlockID(recv.getBlockInfo().getBlockID());
			Socket socket2 = new Socket(recv.getBlockInfo().getHost(), recv.getBlockInfo().getPort());
			FSMessage recv2 = FSMessage.exchangeMessage(socket2, message2);
			if (recv2.getType() == FSMessage.MESSAGE_GETBLOCK_REQ_ACK) {
				FileOperator.recvFileToBuffer(socket2.getInputStream(), buffer, recv.getBlockInfo().getLength());
				socket2.close();
			} else {
				Log.write("Got the block location, but failed to fetch block from datanode.");
				socket2.close();
			}
		} else if (recv.getType() == FSMessage.MESSAGE_BLOCKLOC_FAIL) {
			Log.write("Get block failed.");
		}
		socket.close();
	}
	
	public ArrayList<FileStatus> list(String path) throws IOException, ClassNotFoundException {
		FSMessage message = new FSMessage(FSMessage.MESSAGE_LIST_REQ).withPath(path);
		Socket socket = new Socket(nameNodeHost, nameNodePort);
		FSMessage recv = FSMessage.exchangeMessage(socket, message);
		
		if (recv.getType() == FSMessage.MESSAGE_LIST_REQ_ACK) {
			socket.close();
			return recv.getFileStatusList();
		} else {
			Log.write("List sub-items failed: Path " + path + " does not exist.");
			socket.close();
			return null;
		}
	}
	
	public FileStatus listThis(String path) throws IOException, ClassNotFoundException {
		FSMessage message = new FSMessage(FSMessage.MESSAGE_LIST_REQ).withPath(path);
		Socket socket = new Socket(nameNodeHost, nameNodePort);
		FSMessage recv = FSMessage.exchangeMessage(socket, message);
		
		if (recv.getType() == FSMessage.MESSAGE_LIST_REQ_ACK) {
			socket.close();
			return recv.getFileStatus();
		} else {
			Log.write("List failed.");
			socket.close();
			return null;
		}
	}
	
	private String generateTempFileName(int blockID) {
		Random random = new Random();
		return blockID + "_temp_" + Math.abs(random.nextInt()) % 1000;
	}
}
