package com.erj.practice.hadoop.patent.citations;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * Same output as Citations except that this removes the "CITED" "CITING" line at the top of the output file
 */

@SuppressWarnings("deprecation")
public class CleanCitations extends Configured implements Tool{
	public static class ImTheMap extends MapReduceBase implements Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			// Reverse the key-value pair, strip out the titles
			try{
				if(Integer.parseInt(key.toString()) > 0){
					output.collect(value, key);
				}
			}catch(NumberFormatException e){}
		}
	}
	
	public static class Reducto extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			//Build a comma-separated list of citing patents
			
			String csv = "";
			while (values.hasNext()){
				if(csv.length() > 0){
					csv += ",";
				}
				csv += values.next().toString();
				
			}
			output.collect(key, new Text(csv));
		}
	}
	
	public int run (String [] args) throws Exception {
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, CleanCitations.class);
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setJobName("Citations");
		job.setMapperClass(ImTheMap.class);
		job.setReducerClass(Reducto.class);
		job.setInputFormat(KeyValueTextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.set("key.value.separator.in.input.line", ",");
		
		JobClient.runJob(job);
		
		return 0;
	}
	
	public static void main (String [] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),  new CleanCitations(), args);
		System.exit(res);
	}
}
