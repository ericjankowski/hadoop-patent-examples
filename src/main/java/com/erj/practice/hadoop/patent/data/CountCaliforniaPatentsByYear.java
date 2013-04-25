package com.erj.practice.hadoop.patent.data;

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


@SuppressWarnings("deprecation")
public class CountCaliforniaPatentsByYear extends Configured implements Tool{
	private static IntWritable one = new IntWritable(1);
	
	public static class ImTheMap extends MapReduceBase implements Mapper<Text, Text, IntWritable, IntWritable> {
		public void map(Text key, Text value, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
			//Parse the line and make a tick mark for each Year if state is California
			String line = key.toString();
			line = line.replaceAll("\"", "");
			String [] components = line.split(",");
			try{
				Integer.parseInt(components[0]);
				int year = Integer.parseInt(components[1]);
				if(year > 0 && "CA".equals(components[5])){
					output.collect(new IntWritable(year), one);
				}
			}catch(NumberFormatException e){}
		}
	}
			
	public static class Reducto extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		public void reduce(IntWritable key, Iterator<IntWritable> values, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
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
		JobConf job = new JobConf(conf, CountCaliforniaPatentsByYear.class);
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setJobName("CaliforniaPatentsByYear");
		job.setMapperClass(ImTheMap.class);
		job.setReducerClass(Reducto.class);
		job.setInputFormat(KeyValueTextInputFormat.class);
		job.setOutputFormat(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		JobClient.runJob(job);
		
		return 0;
	}
	
	public static void main (String [] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),  new CountCaliforniaPatentsByYear(), args);
		System.exit(res);
	}
}

