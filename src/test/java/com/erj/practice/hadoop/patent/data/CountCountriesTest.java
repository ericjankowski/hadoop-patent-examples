package com.erj.practice.hadoop.patent.data;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

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


public class CountCountriesTest {
	
	CountCountries.ImTheMap mapper;
	CountCountries.Reducto reducer;
	Text key;
	Text value;
	OutputCollector<Text, IntWritable> output; 
	Reporter reporter;
	Iterator<IntWritable> values;
	
	final List<IntWritable> prep = new ArrayList<IntWritable>();
	
	@SuppressWarnings("unchecked")
	@Before
	public void setUp(){
		mapper = new CountCountries.ImTheMap();
		reducer = new CountCountries.Reducto();
		key = new Text();
		value = new Text();
		
		output = mock(OutputCollector.class);
		
		for(int i=0;i<prep.size();i++){
			prep.remove(0);
		}
		values = new Iterator<IntWritable>(){
			List<IntWritable> list = prep;
						
			public boolean hasNext() {
				return list.size() > 0;
			}

			public IntWritable next() {
				return list.remove(0);
			}

			public void remove() {}
			
		};
	}

	
	@Test
	public void mapCollectsOneTickMarkPerRow() throws IOException {
		key.set("3070801,1963,1096,,\"BE\",\"\",,1,,269,6,69,,1,,0,,,,,,,");
				
		mapper.map(key, value, output, reporter);
		
		verify(output).collect(eq(new Text("BE")), eq(new IntWritable(1)));
	}
	
	@Test
	public void mapShouldNotCollectIfKeyDoesNotContainAPatentNumber() throws IOException {
		key.set("BadDates,1963,1096,,\"BE\",\"\",,1,,269,6,69,,1,,0,,,,,,,");
				
		mapper.map(key, value, output, reporter);
		
		verify(output, never()).collect(any(Text.class), eq(new IntWritable(1)));
	}
	
	@Test
	public void reduceCountsAndCollectsTheNumberOfCitations() throws IOException{
		prep.add(new IntWritable(1));
		prep.add(new IntWritable(1));
		
		reducer.reduce(key, values, output, reporter);
		
		verify(output).collect(eq(key), eq(new IntWritable(2)));
	}
	
	@Test
	public void reduceCountsAndCollectsTheNumberOfCitationsThatCantBeHardCoded() throws IOException{
		int firstNumber = (int)(Math.random() * 100);
		int secondNumber = (int)(Math.random() * 100);
		int sum = firstNumber + secondNumber;
		
		prep.add(new IntWritable(firstNumber));
		prep.add(new IntWritable(secondNumber));
		
		reducer.reduce(key, values, output, reporter);
		
		verify(output).collect(eq(key), eq(new IntWritable(sum)));
		
	}

}
