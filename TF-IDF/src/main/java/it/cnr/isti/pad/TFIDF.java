package it.cnr.isti.pad;

import java.io.IOException;
import java.text.DecimalFormat;

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

public class TFIDF {
	private static final DecimalFormat DF = new DecimalFormat("###.########");
	private static final int fileCount = 2;
	
	public static class MapperTFIDF extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			 String[] wordDocAndNumbers = value.toString().split(":");
			 String[] numbers = wordDocAndNumbers[1].split(";");
			 double idf = (double) fileCount / Double.parseDouble(numbers[2]);
			 double tf = Double.parseDouble(numbers[0]) / Double.parseDouble(numbers[1]);
			 double tfidf = tf * Math.log10(idf);
			 context.write(new Text(wordDocAndNumbers[0]), new Text(DF.format(tfidf)));
		}
	}
	
	public static class ReducerTFIDF extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for(Text value: values)
				context.write(key, value);
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "IDIDF");
		job.setJarByClass(TFIDF.class);
		//job.setNumReduceTasks(2);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MapperTFIDF.class);
		job.setReducerClass(ReducerTFIDF.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
