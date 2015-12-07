package it.cnr.isti.pad;

import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class WordFreqCount 
{

	public static class MapperWord extends Mapper<LongWritable, Text, Text, Text>{
		private Text resultvalue = new Text();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
			Pattern p = Pattern.compile("\\w+"); 
			Matcher m = p.matcher(value.toString()); 
			while (m.find()) { 
			    String word = m.group().toLowerCase();
			    if(!word.contains("_")){
			    	resultvalue.set(fileName);
			    	context.write(resultvalue, new Text(word+";"+"1"));
			    }
			} 
		}
	}
	
	public static class ReducerWord extends Reducer<Text, Text, Text, Text>{
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			HashMap<String, Integer> tempFreq = new HashMap<String,Integer>();
			for(Text value: values){
				String[] wordAndCount = value.toString().split(";");
				sum += Integer.parseInt(wordAndCount[1]);
				if(!tempFreq.containsKey(wordAndCount[0]))
					tempFreq.put(wordAndCount[0], 1);
				else
					tempFreq.put(wordAndCount[0], tempFreq.get(wordAndCount[0])+1);
			}
			for(String keyvalue: tempFreq.keySet())
				context.write(new Text(keyvalue+"@"+key.toString()+":"), new Text(tempFreq.get(keyvalue)+";"+ sum));
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "IDIDF");
		job.setJarByClass(WordFreqCount.class);
		//job.setNumReduceTasks(2);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MapperWord.class);
		job.setReducerClass(ReducerWord.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
    
}
