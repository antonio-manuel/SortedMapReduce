package eu.antoniolopez.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import org.junit.Before;
import org.junit.Test;

import eu.antoniolopez.mapreduce.IntSumReducer;
import eu.antoniolopez.mapreduce.TokenizerMapper;

public class SortedWordCountTest {

	MapDriver<Object, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text,IntWritable,Text,IntWritable> reduceDriver;
	MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

	@Before
	public void setUp() {
		TokenizerMapper mapper = new TokenizerMapper();
		IntSumReducer reducer = new IntSumReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper()  throws IOException{
		mapDriver.withInput(new LongWritable(), new Text("Hello$"));
		mapDriver.withOutput(new Text("hello"), new IntWritable(1));
		mapDriver.runTest();
	}

	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("hello"), values);
		reduceDriver.withOutput(new Text("hello"), new IntWritable(2));
		reduceDriver.runTest();
	}

	@Test
	public void testMapReduce()  throws IOException{
		mapReduceDriver.withInput(new LongWritable(), new Text("hello Hello$"));
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		mapReduceDriver.withOutput(new Text("hello"), new IntWritable(2));
		mapReduceDriver.runTest();
	}

}
