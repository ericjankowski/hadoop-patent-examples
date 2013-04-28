package com.erj.practice.hadoop.patent.data;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Before;
import org.junit.Test;

public class OrganizeByCountryTest {
	
	OrganizeByCountry.ImTheMap mapper;
	OrganizeByCountry.Reducto reducer;
	Text mapKey;
	Text mapValue;
	Text reduceKey;
	OutputCollector<Text, Text> output; 
	Reporter reporter;
	Iterator<Text> values;
	
	final List<Text> prep = new ArrayList<Text>();
	
	@SuppressWarnings("unchecked")
	@Before
	public void setUp(){
		mapper = new OrganizeByCountry.ImTheMap();
		reducer = new OrganizeByCountry.Reducto();
		mapKey = new Text();
		mapValue = new Text();
		reduceKey = new Text("Phil");
		
		output = mock(OutputCollector.class);
		
		for(int i=0;i<prep.size();i++){
			prep.remove(0);
		}
		values = new Iterator<Text>(){
			List<Text> list = prep;
						
			public boolean hasNext() {
				return list.size() > 0;
			}

			public Text next() {
				return list.remove(0);
			}

			public void remove() {}
			
		};
	}

	
	@Test
	public void mapCollectsOneCountryPatentPairPerRow() throws IOException {
		mapKey.set("3070801,1963,1096,,\"US\",\"TX\",,1,,269,6,69,,1,,0,,,,,,,");
				
		mapper.map(mapKey, mapValue, output, reporter);
		
		verify(output).collect(eq(new Text("US")), eq(new Text("3070801")));
	}
	
	@Test
	public void mapShouldNotCollectIfKeyDoesNotContainAPatentNumber() throws IOException {
		mapKey.set("BadDates,1963,1096,,\"US\",\"TX\",,1,,269,6,69,,1,,0,,,,,,,");
				
		mapper.map(mapKey, mapValue, output, reporter);
		
		verify(output, never()).collect(any(Text.class), any(Text.class));
	}
	
	@Test
	public void reduceCountsAndCollectsTheNumberOfPatentsPerCountry() throws IOException{
		prep.add(new Text("Foo"));
		prep.add(new Text("Bar"));
		
		reducer.reduce(reduceKey, values, output, reporter);
		
		verify(output).collect(eq(reduceKey), eq(new Text("Foo,Bar")));
	}
}
