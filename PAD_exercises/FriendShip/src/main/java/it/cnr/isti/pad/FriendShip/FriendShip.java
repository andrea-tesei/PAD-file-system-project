package it.cnr.isti.pad.FriendShip;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

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


public class FriendShip {
	
	static class CommonUserComparator implements Comparator<String>
	 {
	     public int compare(String c1, String c2)
	     {
	    	 int res = Integer.compare(Integer.parseInt(c1.substring(0, c1.indexOf(":"))), Integer.parseInt(c2.substring(0, c2.indexOf(":"))));
	    	 if(res == 0)
	    		 res = -Integer.compare(Integer.parseInt(c1.substring(c1.indexOf(":")+1, c1.length())), Integer.parseInt(c2.substring(c2.indexOf(":") + 1, c2.length())));
	    	 return res;
	     }
	 }
	
	public static class MapperFriend extends Mapper<LongWritable, Text, IntWritable, Text>{
		private Text resultValue = new Text();
		private IntWritable resultKey = new IntWritable(0);
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
			String[] userAndAdiacencyList = value.toString().split("\t");
			if(userAndAdiacencyList.length == 1){
				resultKey.set(Integer.parseInt(userAndAdiacencyList[0]));
				resultValue.set("emptylist");
				context.write(resultKey, resultValue);
				return;
			}
			String[] listOfFriends = userAndAdiacencyList[1].split(",");
			for(int i = 0; i < listOfFriends.length; i ++){
				resultKey.set(Integer.parseInt(userAndAdiacencyList[0]));
				resultValue.set(listOfFriends[i] + ":0"); // Print friend list
				context.write(resultKey, resultValue);
			}
			for(int i = 0; i < listOfFriends.length; i++){
				resultKey.set(Integer.parseInt(listOfFriends[i]));
				for(int j = 0; j < listOfFriends.length; j++){
					if(j != i){
						resultValue.set(listOfFriends[j]+ ":1"); // Print common friend list
						context.write(resultKey, resultValue);
					}
				}
			}
		}
	}
	
	public static class ReducerFriend extends Reducer<IntWritable, Text, IntWritable, Text>{
		//private IntWritable tempValue = new IntWritable(0);
		private Text finalValue = new Text("");
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			System.err.println("Key = " +  key.get() + "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
			int N = 10;
			int i = 0;
			boolean hasKeyEmptyList = false;
			finalValue.set("");
			HashMap<String, Integer> commonFriends = new HashMap<String, Integer>();
			ArrayList<Integer> friendList = new ArrayList<Integer>(); 
			List<String> orderedResults = new ArrayList<String>();
			for(Text txt: values){
				if(!txt.toString().equals("emptylist")){
					String[] commonUser = txt.toString().split(":");
					if(!(Integer.parseInt(commonUser[1]) == 0)){
						if(commonFriends.containsKey(commonUser[0])){
							//System.err.println("Saving pair:  " + commonUser[0] + ", " + (commonFriends.get(commonUser[0]).intValue()+1));
							commonFriends.put(commonUser[0], commonFriends.get(commonUser[0]).intValue()+1);
						} else {
							//System.err.println("Saving pair:  " + commonUser[0] + ", " + "1");
							commonFriends.put(commonUser[0], new Integer(1));
						}
					} else {
						//System.err.println("Saving in friend list:  " + commonUser[0] + ", " + "0");
						friendList.add(new Integer(Integer.parseInt(commonUser[0])));
					}
				} else
					hasKeyEmptyList = true;
			}
			// Sort items in commonFriends according to the value
			for(String user: commonFriends.keySet()){
				//System.err.println("Adding to collection item = " + tempResults.get(user).intValue() + ":" + user);
				orderedResults.add(commonFriends.get(user).intValue() + ":" + user);
			}
			if(!hasKeyEmptyList)
				Collections.sort(orderedResults, Collections.reverseOrder(new CommonUserComparator()));
//			for(int x = 0; x < orderedResults.size(); x++)
//				System.err.println("Ordered partial result = " + orderedResults.get(x));
			while(N > 0 && i < orderedResults.size() && !hasKeyEmptyList){
				// Check if already friend
				if(friendList.indexOf(new Integer(Integer.parseInt(orderedResults.get(i).substring(orderedResults.get(i).indexOf(":") + 1, orderedResults.get(i).length())))) == -1){
					//System.err.println("not already friend");
					finalValue.set(finalValue.toString() + orderedResults.get(i).substring(orderedResults.get(i).indexOf(":")+1, orderedResults.get(i).length()) + ",");
					N--;
				}
				i++;
			}
			context.write(key, finalValue);
		}
	}
	
	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "friendship");
		job.setJarByClass(FriendShip.class);
		//job.setNumReduceTasks(4);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MapperFriend.class);
		job.setReducerClass(ReducerFriend.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}