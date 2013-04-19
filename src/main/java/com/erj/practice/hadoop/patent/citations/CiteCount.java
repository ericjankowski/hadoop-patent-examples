package com.erj.practice.hadoop.patent.citations;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

import com.erj.practice.hadoop.patent.citations.CleanCitations.ImTheMap;
import com.erj.practice.hadoop.patent.citations.CleanCitations.Reducto;


/*
 * This MapReduce job counts the number of citations a particular patent makes.  It outputs a patent number and the number of patents that it cites from cite75_99
 * 
 */
public class CiteCount extends Configured implements Tool{
	public static class ImTheMap extends MapReduceBase implements Mapper<Text, Text, Text, IntWritable> {
		public void map(Text key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			//Tick mark for each citation a patent makes
			try{
				if(Integer.parseInt(key.toString())>0 && Integer.parseInt(value.toString())>0){
					output.collect(key, new IntWritable(1));
				}
			}catch(NumberFormatException e){
				//Don't collect anything if key is not a patent number
			}
		}
	}
	
	public static class Reducto extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			//Add up the tick marks
			int count = 0;
			while (values.hasNext()){
				count += values.next().get();
			}
			output.collect(key, new IntWritable(count));
		}
	}
	
	public int run (String [] args) throws Exception {
		Configuration conf = getConf();
		JobConf job = new JobConf(conf, CiteCount.class);
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setJobName("CiteCount");
		job.setMapperClass(ImTheMap.class);
		job.setReducerClass(Reducto.class);
		job.setInputFormat(KeyValueTextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.set("key.value.separator.in.input.line", ",");
		
		JobClient.runJob(job);
		
		return 0;
	}
	
	public static void main (String [] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),  new CiteCount(), args);
		System.exit(res);
	}
}
