package eu.antoniolopez.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import org.junit.Before;
import org.junit.Test;

import eu.antoniolopez.mapreduce.mapper.NumberMapper;
import eu.antoniolopez.mapreduce.reducer.RepeatReducer;

public class SortCountTest {

	MapDriver<Object, Text, LongWritable, Text> mapDriver;
	ReduceDriver<LongWritable,Text,LongWritable,Text> reduceDriver;
	MapReduceDriver<Object, Text, LongWritable, Text, LongWritable, Text> mapReduceDriver;

	@Before
	public void setUp() {
		NumberMapper mapper = new NumberMapper();
		RepeatReducer reducer = new RepeatReducer();
		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testSortMapper()  throws IOException{
		List<Pair<Object,Text>> list = new ArrayList<Pair<Object,Text>>();
		list.add(new Pair<Object,Text>(new LongWritable(), new Text("goodbye 9")));
		list.add(new Pair<Object,Text>(new LongWritable(), new Text("hello 10")));
		mapDriver.addAll(list);
		mapDriver.withOutput(new LongWritable(9),new Text("goodbye"));
		mapDriver.withOutput(new LongWritable(10),new Text("hello"));
		mapDriver.runTest();
	}

	@Test
	public void testSortReducer() throws IOException {
		List<Pair<LongWritable,List<Text>>> list = new ArrayList<Pair<LongWritable,List<Text>>>();
		List<Text> words = new ArrayList<Text>();
		words.add(new Text("goodbye"));
		list.add(new Pair<LongWritable,List<Text>>(new LongWritable(9), words));
		words = new ArrayList<Text>();
		words.add(new Text("hello"));
		list.add(new Pair<LongWritable,List<Text>>(new LongWritable(10), words));
		reduceDriver.addAll(list);
		reduceDriver.withOutput(new LongWritable(9),new Text("goodbye"));
		reduceDriver.withOutput(new LongWritable(10),new Text("hello"));
		reduceDriver.runTest();
	}

	@Test
	public void testSortMapReduce()  throws IOException{
		List<Pair<Object,Text>> list = new ArrayList<Pair<Object,Text>>();
		list.add(new Pair<Object,Text>(new LongWritable(), new Text("goodbye 9")));
		list.add(new Pair<Object,Text>(new LongWritable(), new Text("hello 10")));
		mapReduceDriver.addAll(list);
		mapReduceDriver.withOutput(new LongWritable(9),new Text("goodbye"));
		mapReduceDriver.withOutput(new LongWritable(10),new Text("hello"));
		mapReduceDriver.runTest();
	}

}
