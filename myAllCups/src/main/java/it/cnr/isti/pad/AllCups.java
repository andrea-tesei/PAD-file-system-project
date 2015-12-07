package it.cnr.isti.pad;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AllCups {
	
	public static class MapperCups extends Mapper<Object, Text, IntWritable, Text>{
		private static IntWritable resultkey = new IntWritable(0);
		private Text resultvalue = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			resultvalue.set(value.toString().toUpperCase());
			context.write(resultkey, resultvalue);
			resultkey.set(resultkey.get()+1);
		}
	}
	
	public static class ReducerCups extends Reducer<IntWritable, Text, IntWritable, Text>{
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			for(Text txt: values)
				context.write(key, txt);
		}
	}
	
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "touppercase");
		job.setJarByClass(AllCups.class);
		job.setNumReduceTasks(2);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MapperCups.class);
		job.setReducerClass(ReducerCups.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
