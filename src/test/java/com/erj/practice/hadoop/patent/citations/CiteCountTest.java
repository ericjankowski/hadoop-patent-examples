package com.erj.practice.hadoop.patent.citations;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;


public class CiteCountTest {
	
	CiteCount.ImTheMap mapper;
	Text key;
	Text value;
	OutputCollector<Text, IntWritable> output; 
	Reporter reporter;
	
	@SuppressWarnings("unchecked")
	@Before
	public void setUp(){
		mapper = new CiteCount.ImTheMap();
		key = new Text();
		value = new Text();
		
		output = mock(OutputCollector.class);
	}

	@Test
	public void mapCollectsOneTickMarkPerCitation() throws IOException {
		key.set("12345");
		value.set("54321");
				
		mapper.map(key, value, output, reporter);
		
		verify(output).collect(eq(key), eq(new IntWritable(1)));
	}
	
	@Test
	public void mapShouldNotCollectIfKeyIsNotAPatentNumber() throws IOException {
		key.set("ThisAintANumber");
		value.set("12345");
				
		mapper.map(key, value, output, reporter);
		
		verify(output, never()).collect(eq(key), eq(new IntWritable(1)));
	}
	
	@Test
	public void mapShouldNotCollectIfValueIsNotAPatentNumber() throws IOException {
		key.set("12345");
		value.set("NorIsThisANumber");
				
		mapper.map(key, value, output, reporter);
		
		verify(output, never()).collect(eq(key), eq(new IntWritable(1)));
	}
	
	@SuppressWarnings("resource")
	@Test
	public void reduceCountsAndCollectsTheNumberOfCitations() throws IOException{
		
		CiteCount.Reducto reducer = new CiteCount.Reducto();
		final List<IntWritable> prep = new ArrayList<IntWritable>();
		prep.add(new IntWritable(1));
		prep.add(new IntWritable(1));
		
		Iterator<IntWritable> values = new Iterator<IntWritable>(){
			List<IntWritable> list = prep;
						
			public boolean hasNext() {
				return list.size() > 0;
			}

			public IntWritable next() {
				return list.remove(0);
			}

			public void remove() {}
			
		};
		
		reducer.reduce(key, values, output, reporter);
		
		verify(output).collect(eq(key), eq(new IntWritable(2)));
	}
	
	//  This test is here in case Dan Watt or Jason McIntosh ever read this.
	//  You both taught me a lot, and I had a blast programming with both of you.
	//  Cheers!!
	@SuppressWarnings("resource")
	@Test
	public void reduceCountsAndCollectsTheNumberOfCitationsThatCantBeHardCoded() throws IOException{
		int firstNumber = (int)(Math.random() * 100);
		int secondNumber = (int)(Math.random() * 100);
		int sum = firstNumber + secondNumber;
		CiteCount.Reducto reducer = new CiteCount.Reducto();
		final List<IntWritable> prep = new ArrayList<IntWritable>();
		prep.add(new IntWritable(firstNumber));
		prep.add(new IntWritable(secondNumber));
		
		Iterator<IntWritable> values = new Iterator<IntWritable>(){
			List<IntWritable> list = prep;
						
			public boolean hasNext() {
				return list.size() > 0;
			}

			public IntWritable next() {
				return list.remove(0);
			}

			public void remove() {}
			
		};
		
		reducer.reduce(key, values, output, reporter);
		
		verify(output).collect(eq(key), eq(new IntWritable(sum)));
		
	}
}
