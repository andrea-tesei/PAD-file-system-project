package it.cnr.isti.pad;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordFrequencyCOLL {
	public static class MapperFreq extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			 String[] pair = value.toString().split("@");
			 context.write(new Text(pair[0]), new Text(pair[1]));
		}
	}
	
	public static class ReducerFreq extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int mtemp = 0;
			HashMap<String,String> tempFreq = new HashMap<String,String>();
			for(Text value: values){
				mtemp++;
				String[] docNameAndNumbers = value.toString().split(":");
				tempFreq.put(docNameAndNumbers[0], docNameAndNumbers[1]);
			}
			for(String docvalue: tempFreq.keySet()){
				context.write(new Text(key.toString() + "@" + docvalue + ":"), new Text(tempFreq.get(docvalue)+ ";" + mtemp));
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "WordFrequencyCOLL");
		job.setJarByClass(WordFrequencyCOLL.class);
		//job.setNumReduceTasks(2);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MapperFreq.class);
		job.setReducerClass(ReducerFreq.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
