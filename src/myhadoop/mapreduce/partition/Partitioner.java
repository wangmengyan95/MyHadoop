package myhadoop.mapreduce.partition;

/**
 * Calculate the partition for a key.
 */
public class Partitioner<KEY, VALUE> {
	public int getPartition(KEY key, VALUE value, int partitions) {
		if (partitions > 0)
			return Math.abs(key.hashCode()) % partitions;
		return -1;
	}
}
