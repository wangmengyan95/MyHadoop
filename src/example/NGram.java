package example;

import java.io.IOException;
import java.util.ArrayList;
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

public class NGram {
	public static class Map extends Mapper<Long, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private final static int MAX_N = 3;

		public void map(Long key, Text value, MapContext<Long, Text, Text, IntWritable> context) throws IOException {
			ArrayList<String> strs = new ArrayList<String>();
			
			// Replace all punctuation with spaces.
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line.replaceAll("[^a-zA-Z]", " "));			
			while (tokenizer.hasMoreTokens()) {
				strs.add(tokenizer.nextToken().toLowerCase().trim());
			}
			
			// Generate the unreduced n-grams.
			int len = strs.size();			
			for (int n = 1; n <= MAX_N; n++) {
				for (int i = 0; i <= len - n; i++) {
					StringBuilder sb = new StringBuilder();
					for (int j = 0; j < n; j++)
						sb.append(" " + strs.get(i + j));
					Text word = new Text(sb.toString().trim());
					context.write(word, one);
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	    public void reduce(Text key, Iterable<IntWritable> values, ReduceContext<Text, IntWritable, Text, IntWritable> context) throws IOException {
	    	// Get the sum of all appearances of one n-gram.
	        int sum = 0;
	        for (IntWritable i : values) {
	            sum += i.getValue();
	        }
	        context.write(key, new IntWritable(sum));
	    }
	 }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "n-gram");
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setInputPath("input");
		job.setOutputPath("ngram-output");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setNumReduceTasks(3);
		
		job.setJarClass(NGram.class);
		
		job.submitJob();
	}
}
