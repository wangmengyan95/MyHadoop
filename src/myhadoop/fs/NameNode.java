package myhadoop.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

import myhadoop.utility.FileOperator;
import myhadoop.utility.Log;

public class NameNode implements Runnable {

	public static final int ITEM_FILE = 1;
	public static final int ITEM_DIR = 2;

	public static final int MAX_CHUNK_SIZE = 65536;				// Maximum size for each chunk(block) in DFS.		
	public static final int MAX_CHUNK_NUMBER = 10000;
	public static final int BUFFER_SIZE = MAX_CHUNK_SIZE + 10;

	public static final int DEFAULT_REPLICA = 2;				// Default replica for the file system.

	ArrayList<DataNodeRef> dataNodes;
	DataItem root;
	Integer nextFileSequenceNumber;
	Integer nextDataNodeID;

	int port;

	public NameNode(int _port) {
		dataNodes = new ArrayList<DataNodeRef>();
		root = new DataItem(ITEM_DIR, "ROOT");
		nextFileSequenceNumber = 0;
		nextDataNodeID = 0;

		port = _port;

		Log.write("Namenode launched: " + port); /* for log */
	}

	/**
	 * Data structure that maintains file hierachy.
	 */
	static class DataItem {
		int type;
		String name;
		long length;
		ArrayList<DataItem> subItems;
		ArrayList<DataBlock> subBlocks;

		public DataItem(int _type, String _name) {
			type = _type;
			name = _name;
			subItems = new ArrayList<DataItem>();
			subBlocks = new ArrayList<DataBlock>();
		}

		// Whether the current directory contains an item with certain name.
		public boolean contains(String subName) {
			if (type == NameNode.ITEM_DIR) {
				for (DataItem i : subItems) {
					if (i.getName().equals(subName))
						return true;
				}
			}
			return false;
		}

		// Get a sub-item of current item with certain name and type.
		public DataItem getSubItem(String subName, int type) {
			if (type == NameNode.ITEM_DIR) {
				for (DataItem i : subItems) {
					if (i.getName().equals(subName) && i.getType() == type)
						return i;
				}
			}
			return null;
		}

		public boolean isDir() {
			return type == ITEM_DIR;
		}

		public int getType() {
			return type;
		}

		public String getName() {
			return name;
		}

		public long getLength() {
			return length;
		}

		public ArrayList<DataItem> getSubItems() {
			if (type == NameNode.ITEM_DIR)
				return subItems;
			return null;
		}

		public ArrayList<DataBlock> getSubBlocks() {
			if (type == NameNode.ITEM_FILE)
				return subBlocks;
			return null;
		}

		public void addItem(DataItem e) {
			if (type == NameNode.ITEM_DIR)
				subItems.add(e);
		}

		public void addBlock(DataBlock b) {
			if (type == NameNode.ITEM_FILE)
				subBlocks.add(b);
		}

		public void setLength(long _length) {
			length = _length;
		}

		// Return the item with relative path @path to current item.
		// If the item does not exist, return null.
		public DataItem navigate(String path) {
			if (path.equals(".") || path.length() == 0)
				return this;
			if (path.charAt(0) == '.')
				return navigate(path.substring(1));
			
			path = removeSlash(path);
			if (path.length() == 0)
				return this;
			int pos = path.indexOf('/');
			pos = pos == -1 ? path.length() : pos;
			String toNavigate = path.substring(0, pos);

			for (DataItem i : subItems) {
				if (i.getName().equals(toNavigate)) {
					if (pos >= path.length() - 1)
						return i;
					else
						return i.navigate(path.substring(pos));
				}
			}

			return null;
		}

		// Print the file hierarchy with current item as root.
		public void print(int pad) {
			for (int i = 0; i < pad; i++)
				System.out.print("\t");
			Log.write(name);
			for (DataItem i : subItems)
				i.print(pad + 1);
		}
	}

	/**
	 * Data structure that maintains basic information of a block(chunk), including ID, length and locations.
	 */
	static class DataBlock {
		int blockID;
		long length;
		long offset;
		ArrayList<DataNodeRef> refs;

		public DataBlock(int _blockID, long _length, long _offset) {
			blockID = _blockID;
			length = _length;
			offset = _offset;
			refs = new ArrayList<DataNodeRef>();
		}

		public void addRef(String host, int port) {
			DataNodeRef toAdd = new DataNodeRef(host, port);

			boolean add = true;
			for (DataNodeRef r : refs)
				if (r.equals(toAdd))
					add = false;

			if (add)
				refs.add(toAdd);
		}

		public int getBlockID() {
			return blockID;
		}

		public long getLength() {
			return length;
		}

		public long getOffset() {
			return offset;
		}

		public int getReplica() {
			return refs.size();
		}

		public ArrayList<DataNodeRef> getRefs() {
			return refs;
		}

	}

	public void addDataNode(String host, int port) {
		DataNodeRef toAdd = new DataNodeRef(host, port);
		boolean add = true;
		for (DataNodeRef r : dataNodes)
			if (r.equals(toAdd))
				add = false;

		if (add)
			dataNodes.add(toAdd);
	}

	// Remove prefix and suffix '/'s of a path.
	private static String removeSlash(String str) {
		if (str.length() > 0 && str.charAt(0) == '/')
			str = str.substring(1);
		if (str.length() > 0 && str.charAt(str.length() - 1) == '/')
			str = str.substring(0, str.length() - 1);
		return str;
	}

	// Make a directory under DataItem @parent.
	private void mkdir(DataItem parent, String name) {
		name = removeSlash(name);
		int firstSlash = name.indexOf('/');
		String dirName = firstSlash == -1 ? name : name
				.substring(0, firstSlash);

		DataItem sub = parent.getSubItem(dirName, NameNode.ITEM_DIR);
		if (sub == null) {
			sub = new DataItem(NameNode.ITEM_DIR, dirName);
			parent.addItem(sub);
		}
		String subName = name.substring(firstSlash + 1);
		if (firstSlash == -1 || firstSlash == name.length() - 1)
			return;

		mkdir(sub, subName);
	}

	private int addFileSequenceNumber() {
		return ++nextFileSequenceNumber;
	}

	// Randomly choose N(N=DEFAULT_REPLICA) data nodes that a block could be replicated in.
	// If the number of data nodes is less than N, returns references to all data nodes.
	private ArrayList<DataNodeRef> chooseNodesForBlock() {
		if (DEFAULT_REPLICA >= dataNodes.size())
			return dataNodes;

		ArrayList<DataNodeRef> ret = new ArrayList<DataNodeRef>();
		HashSet<Integer> added = new HashSet<Integer>();
		Random random = new Random(System.currentTimeMillis());
		int count = 0;
		while (count < DEFAULT_REPLICA) {
			int next = Math.abs(random.nextInt()) % dataNodes.size();
			if (!added.contains(next)) {
				added.add(next);
				ret.add(dataNodes.get(next));
				count++;
			}
		}

		return ret;
	}

	// Check whether the item corresponding to certain path has same type as @type.
	private boolean checkPath(String path, int type) {
		DataItem toCheck = root.navigate(path);
		if (toCheck == null || toCheck.type != type)
			return false;
		return true;
	}

	// Get the father DataItem of the DataItem with a certain path.
	private DataItem getParentFolder(String path) {
		if (path.charAt(path.length() - 1) == '/')
			path = path.substring(0, path.length() - 1);

		int lastSlashIndex = path.lastIndexOf('/');
		if (lastSlashIndex == -1)
			return root;
		else
			return root.navigate(path.substring(0, lastSlashIndex));
	}

	// Generate the FileStatus object of a DataItem of a file.
	private static FileStatus generateFileStatus(String path, DataItem item) {
		if (item == null)
			return null;
		FileStatus ret = new FileStatus(path, item.isDir(), item.getLength(),
				MAX_CHUNK_SIZE); /* length for DIR */
		if (item.getType() == ITEM_FILE) {
			ArrayList<BlockLocation> blockLocs = new ArrayList<BlockLocation>();
			for (DataBlock b : item.getSubBlocks()) {
				ArrayList<String> hostlist = new ArrayList<String>();
				ArrayList<Integer> portlist = new ArrayList<Integer>();
				for (DataNodeRef r : b.getRefs()) {
					hostlist.add(r.getHost());
					portlist.add(r.getPort());
				}

				BlockLocation loc = new BlockLocation(hostlist, portlist,
						b.getOffset(), b.getLength());
				blockLocs.add(loc);
			}
			ret.setBlockLocations(blockLocs);
		}
		return ret;
	}

	// Generate a list of FileStatus object of a DataItem of a directory.
	private static ArrayList<FileStatus> generateFileStatusList(String path,
			DataItem item) {
		if (item == null || item.getType() != ITEM_DIR)
			return null;
		ArrayList<FileStatus> ret = new ArrayList<FileStatus>();
		String prefix = removeSlash(path) + "/";
		for (DataItem i : item.getSubItems())
			ret.add(generateFileStatus(prefix + i.getName(), i));
		return ret;
	}

	// Choose a data node for a block so that user could get the block on that data node.
	// Currently we do not support localization, only choose the first available data node that stores the block.
	private DataNodeRef chooseBlockLocation(DataBlock block) {
		return block.getRefs().get(0);
	}

	public static void main(String[] args) {
		NameNode name = new NameNode(2014);
		new Thread(name).start();
	}

	/*
	 * Listen to requests from clients.
	 */
	public void run() {
		try {
			ServerSocket listenSocket = new ServerSocket(port);
			while (true) {
				Socket socket = listenSocket.accept();

				ObjectOutputStream output = new ObjectOutputStream(
						socket.getOutputStream());
				output.flush();
				ObjectInputStream input = new ObjectInputStream(
						socket.getInputStream());

				FSMessage message = FSMessage.readMessage(input);

				if (message.getType() == FSMessage.MESSAGE_UPLOAD_REQ) {
					// Client wants to upload a file.
					if (!checkPath(
							FileOperator.getParentPath(message.getPath()),
							ITEM_DIR)) {
						// The parent path does not exist.
						FSMessage ack = new FSMessage(
								FSMessage.MESSAGE_UPLOAD_FAIL);
						output.writeObject(ack);
					} else {

						// Create a new DataItem of the file.
						DataItem parent = getParentFolder(message.getPath());
						// If file is duplicated, delete the old one.
						if (parent.contains(FileOperator.getFileName(message.getPath()))) {
							int toDelete = -1;
							for (int i = 0; i < parent.subItems.size(); i++) {
								if (parent.subItems.get(i).getName().equals(FileOperator.getFileName(message.getPath()))
										&& parent.subItems.get(i).getType() == ITEM_FILE)
									toDelete = i;
							}
							if (toDelete > 0)
								parent.subItems.remove(toDelete);
						}
						DataItem newFile = new DataItem(ITEM_FILE,
								FileOperator.getFileName(message.getPath()));
						newFile.setLength(message.getLength());
						parent.addItem(newFile);

						FSMessage ack = new FSMessage(
								FSMessage.MESSAGE_UPLOAD_REQ_ACK);
						output.writeObject(ack);

						// Open a thread for receiving and dispatching the file to data nodes.
						new Thread(new FileUploader(socket.getInputStream(),
								newFile, message.getLength())).start();
					}
				} else if (message.getType() == FSMessage.MESSAGE_BLOCKLOC_REQ) {
					// Client wants to know the location of the block that specified offset of a file lies in.
					if (!checkPath(message.getPath(), ITEM_FILE)
							|| message.getPosition() > root.navigate(
									message.getPath()).getLength()
							|| message.getPosition() < 0) {
						FSMessage ack = new FSMessage(
								FSMessage.MESSAGE_BLOCKLOC_FAIL);
						output.writeObject(ack);
					} else {

						int blockNumber = (int) message.getPosition()
								/ MAX_CHUNK_SIZE;
						DataItem item = root.navigate(message.getPath());
						DataBlock block = item.getSubBlocks().get(blockNumber);
						DataNodeRef ref = chooseBlockLocation(block);
						FSMessage ack = new FSMessage(
								FSMessage.MESSAGE_BLOCKLOC_REQ_ACK)
								.withBlockInfo(new BlockInfo(
										block.getBlockID(), ref.getHost(), ref
												.getPort(), block.getLength()));
						output.writeObject(ack);
					}
				} else if (message.getType() == FSMessage.MESSAGE_LIST_REQ) {
					// Client wants to know meta information of a certain path.
					DataItem item = root.navigate(message.getPath());

					// The path is not valid.
					if (item == null) {
						FSMessage ack = new FSMessage(
								FSMessage.MESSAGE_LIST_FAIL);
						output.writeObject(ack);
					} else {
						FSMessage ack = new FSMessage(
								FSMessage.MESSAGE_LIST_REQ_ACK)
								.withFileStatus(generateFileStatus(
										message.getPath(), item));
						if (item.getType() == ITEM_DIR) {
							ack.withFileStatusList(generateFileStatusList(
									message.getPath(), item));
						}

						output.writeObject(ack);
					}
				} else if (message.getType() == FSMessage.MESSAGE_MKDIR_REQ) {
					// Client wants to make a new folder.
					if (root.navigate(message.getPath()) != null) {
						FSMessage ack = new FSMessage(
								FSMessage.MESSAGE_MKDIR_FAIL);
						output.writeObject(ack);
					} else {
						mkdir(root, message.getPath());
						FSMessage ack = new FSMessage(
								FSMessage.MESSAGE_MKDIR_REQ_ACK);
						output.writeObject(ack);
					}
				} else if (message.getType() == FSMessage.MESSAGE_DATANODE_REQ) {
					// DataNode wants to get a ID.
					dataNodes.add(new DataNodeRef(message.getNodeHost(), message.getPort()));

					int id;
					synchronized (nextDataNodeID) {
						id = nextDataNodeID;
						nextDataNodeID++;
					}

					Log.write("New datanode[Addr: "
							+ message.getNodeHost()
							+ ", Port:" + message.getPort() + ") added, ID: "
							+ id);
					FSMessage ack = new FSMessage(
							FSMessage.MESSAGE_DATANODE_REQ_ACK).withNodeID(id);
					output.writeObject(ack);
				} else if (message.getType() == FSMessage.MESSAGE_DATANODE_HEARTBEAT) {
					// DataNode heart-beat.
					// Do nothing.
				}

				if (message.getType() != FSMessage.MESSAGE_UPLOAD_REQ)
					socket.close();
			}

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Receive the file uploaded by client and dispatch chunks to data nodes.
	 */
	class FileUploader implements Runnable {
		InputStream in;
		DataItem file;
		long length;

		FileUploader(InputStream _in, DataItem _file, long _length) {
			in = _in;
			file = _file;
			length = _length;
		}

		public void run() {
			try {
				int sequenceNumber = addFileSequenceNumber();

				int nParts = (int) length / MAX_CHUNK_SIZE;
				for (int i = 0; i < nParts + 1; i++) {
					int size = i == nParts ? ((int) length % MAX_CHUNK_SIZE)
							: MAX_CHUNK_SIZE;
					long offset = i * MAX_CHUNK_SIZE;
					ArrayList<DataNodeRef> refs = chooseNodesForBlock();
					int blockID = sequenceNumber * MAX_CHUNK_NUMBER + i;
					DataBlock block = new DataBlock(blockID, size, offset);
					
					byte[] buffer = new byte[BUFFER_SIZE];
					// Receive the chunk to a buffer.
					long len = FileOperator.recvFileToBuffer(in, buffer, size);

					// Send the chunk to selected data nodes.
					for (DataNodeRef r : refs) {
						block.addRef(r.getHost(), r.getPort());
						Socket sendSocket = new Socket(r.getHost(), r.getPort());
						FSMessage message = new FSMessage(
								FSMessage.MESSAGE_SPREAD_REQ)
								.withBlockID(blockID).withLength(size)
								.withOffset(offset);
						FSMessage recv = FSMessage.exchangeMessage(sendSocket,
								message);

						if (recv.getType() == FSMessage.MESSAGE_SPREAD_REQ_ACK) {
							FileOperator.dispatchFile(buffer,
									sendSocket.getOutputStream(), len);
						} else {
							Log.write("Spread block " + blockID + " failed.");
						}
						sendSocket.close();
					}
					file.addBlock(block);
				}

			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
	}
}