package myhadoop.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;

import myhadoop.utility.FileOperator;
import myhadoop.utility.Log;

public class DataNode implements Runnable {
	
	ConcurrentHashMap<Integer, DataBlock> blockMap;
	int port;
	int nodeID;
	
	String nameNodeHost;
	int nameNodePort;
	
	public static final int HEARTBEAT_INTERVAL = 5000;
	
	public DataNode(String _nameNodeHost, int _nameNodePort, int _port) throws Exception {
		blockMap = new ConcurrentHashMap<Integer, DataBlock>();
		port = _port;
		
		nameNodeHost = _nameNodeHost;
		nameNodePort = _nameNodePort;
		
		nodeID = getDataNodeID();
		
		// Create datanode dir.
		FileSystem local = FileSystem.getFileSystem("local");
		local.mkdir(new Path("Datanode_" + nodeID));
		
		new Thread(new HeartBeater()).start();
		Log.write("Datanode " + nodeID + " is launched: " + port);	/* for log */
	}
	
	/*
	 * Data structure that stores basic information about a block.
	 */
	static class DataBlock {
		int blockID;
		String blockName;	// The location that the block resides in the file system.
		long length;
		long offset;
		
		public DataBlock(int _blockID, String _blockName, long _length, long _offset) {
			blockID = _blockID;
			blockName = _blockName;
			length = _length;
			offset = _offset;
		}
		
		public int getBlockID() {
			return blockID;
		}
		
		public String getBlockName() {
			return blockName;
		}		
		
		public long getLength() {
			return length;
		}
		
		public long getOffset() {
			return offset;
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			System.out.println("Usage: java myhadoop.fs.DataNode <NameNodeHost> <DataNodePort>");
			System.out.println("No argument specified, launch 2 datanodes by default.");
			new Thread(new DataNode("localhost", 2014, 20001)).start();
			new Thread(new DataNode("localhost", 2014, 20003)).start();
		} else if (args.length == 2) {
			new Thread(new DataNode(args[0], 2014, Integer.parseInt(args[1]))).start();
		}
	}

	public void run() {
		/*
		 * Listen to requests from clients and namenode.
		 */
		try {
			ServerSocket listenSocket = new ServerSocket(port);
			while (true) {
				Socket socket = listenSocket.accept();
				
				ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
				output.flush();
				ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
				
				FSMessage message = FSMessage.readMessage(input);
				
				if (message.getType() == FSMessage.MESSAGE_SPREAD_REQ) {
					// Namenode wants to send a block to datanode.
					FSMessage ack = new FSMessage(FSMessage.MESSAGE_SPREAD_REQ_ACK);
					output.writeObject(ack);
					
					// Open a new thread to receive the block.
					new Thread(new BlockReceiver(socket.getInputStream(), message.getBlockID(), message.getLength(), message.getOffset())).start();
				} else if (message.getType() == FSMessage.MESSAGE_GETBLOCK_REQ) {
					// Clients wants to get a block with certain block ID.
					if (!blockMap.containsKey(message.getBlockID())) {
						FSMessage ack = new FSMessage(FSMessage.MESSAGE_GETBLOCK_FAIL);
						output.writeObject(ack);
						break;
					} 
					
					// Open a new thread to send the block.
					FSMessage ack = new FSMessage(FSMessage.MESSAGE_GETBLOCK_REQ_ACK);
					output.writeObject(ack);
					new Thread(new BlockSender(socket.getOutputStream(), message.getBlockID())).start();
				}
					
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} 
	}
	
	private String generateDataNodeFileName(int blockID) {
		return "Datanode_" + nodeID + "/" + nodeID + "_" + blockID;
	}
	
	/*
	 * Acquire datanode ID when being initialized.
	 */
	public int getDataNodeID() throws Exception {
		Socket socket = new Socket(nameNodeHost, nameNodePort);
		FSMessage message = new FSMessage(FSMessage.MESSAGE_DATANODE_REQ)
				.withPort(port)
				.withNodeHost(InetAddress.getLocalHost().getHostAddress().toString());
		FSMessage recv = FSMessage.exchangeMessage(socket, message);
		
		int ret;
		if (recv.getType() == FSMessage.MESSAGE_DATANODE_REQ_ACK)
			ret = recv.getNodeID();
		else
			ret = -1;
		socket.close();
		return ret;
	}
	
	/*
	 * Checks whether the namenode is alive.
	 */
	class HeartBeater implements Runnable {
		public void run() {
			try {
				while (true) {
					Thread.sleep(HEARTBEAT_INTERVAL);
					
					Socket socket = new Socket(nameNodeHost, nameNodePort);
					ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());					
					FSMessage message = new FSMessage(FSMessage.MESSAGE_DATANODE_HEARTBEAT).withNodeID(nodeID);
					output.writeObject(message);
					socket.close();
				}
			} catch (Exception e) {
				Log.write("Can not get access to name node, data node failed.");
			}
		}
	}
	
	/*
	 * Thread for receiving a block from namenode.
	 */
	class BlockReceiver implements Runnable {
		InputStream in;
		int blockID;
		long length;
		long offset;
		
		BlockReceiver(InputStream _in, int _blockID, long _length, long _offset) {
			in = _in;
			blockID = _blockID;
			length = _length;
			offset = _offset;
		}
		
		public void run() {
			try {
				String fileName = generateDataNodeFileName(blockID);
				FileOperator.recvFileWithLength(in, fileName, length);
				
				blockMap.put(blockID, new DataBlock(blockID, fileName, length, offset));
			} catch (IOException e) {
				Log.write("Receiving block failed.");
			}
		}
	}
	
	/*
	 * Thread for sending a block to client.
	 */
	class BlockSender implements Runnable {
		OutputStream out;
		int blockID;
		
		BlockSender(OutputStream _out, int _blockID) {
			out = _out;
			blockID = _blockID;
		}
		
		public void run() {
			try {
				String fileName = blockMap.get(blockID).getBlockName();
				FileOperator.sendFile(out, fileName);
			} catch (IOException e) {
				Log.write("Sending block failed");
			}
		}
	}
}
