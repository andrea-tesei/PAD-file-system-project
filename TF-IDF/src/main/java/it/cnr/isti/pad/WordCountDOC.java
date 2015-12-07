package it.cnr.isti.pad;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountDOC {
	
	public static class MapperWordCount extends Mapper<LongWritable, Text, Text, Text>{
		private Text docresult = new Text("");
		private Text valueresult = new Text("");
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String[] docname = value.toString().split("@");
			String aux = "";
			docresult.set(docname[0]);
			StringTokenizer itr = new StringTokenizer(docname[1]);
            while (itr.hasMoreTokens())
            	aux = aux + itr.nextToken() + ";"; 
            valueresult.set(aux);
            context.write(docresult, valueresult);
		}
	}
	
	
	public static class ReducerWordCount extends Reducer<Text, Text, Text, Text>{
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			HashMap<String, Integer> tempCount = new HashMap<String,Integer>();
			for(Text value: values){	
				sum += Integer.parseInt(value.toString().substring(value.toString().indexOf(";") + 1, value.toString().lastIndexOf(";")));
				tempCount.put(value.toString().substring(0, value.toString().indexOf(";")) + "@" + key.toString() + ":", Integer.parseInt(value.toString().substring(value.toString().indexOf(";") + 1, value.toString().lastIndexOf(";"))));
			}
			for(String word : tempCount.keySet())
				context.write(new Text(word), new Text(tempCount.get(word).intValue() + ";" + sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "IDIDF");
		job.setJarByClass(WordFreqCount.class);
		//job.setNumReduceTasks(2);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MapperWordCount.class);
		job.setReducerClass(ReducerWordCount.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
