package example;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.StringTokenizer;

import myhadoop.job.Job;
import myhadoop.mapreduce.input.TextInputFormat;
import myhadoop.mapreduce.io.IntWritable;
import myhadoop.mapreduce.io.Text;
import myhadoop.mapreduce.map.MapContext;
import myhadoop.mapreduce.map.Mapper;
import myhadoop.mapreduce.output.TextOutputFormat;
import myhadoop.mapreduce.reduce.ReduceContext;
import myhadoop.mapreduce.reduce.Reducer;
import myhadoop.utility.Configuration;

public class WordCount {
	public static class Map extends Mapper<Long, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		
		@Override
		public void map(Long key, Text value,
				MapContext<Long, Text, Text, IntWritable> context) throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				Text word = new Text(tokenizer.nextToken());
				// Remove all non-characters.
				String s = word.getText().toLowerCase().replaceAll("[^a-z]+", "");
				if (s.length() > 0) {
					context.write(new Text(s), one);
				}
			}		
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				ReduceContext<Text, IntWritable, Text, IntWritable> context) throws IOException {
			int sum = 0;
			for (IntWritable i : values)
				sum += i.getValue();
			context.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws RemoteException, NotBoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "wordcount");
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setInputPath("input");
		job.setOutputPath("wordcount-output");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setNumReduceTasks(2);
		
		job.setJarClass(WordCount.class);
		
		job.submitJob();
	}
}
