package eu.antoniolopez.mapreduce.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NumberMapper  extends Mapper<Object, Text, LongWritable, Text>{

	private final static LongWritable number = new LongWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
		StringTokenizer st = new StringTokenizer(value.toString());
		while (st.hasMoreTokens()) {
			word.set(st.nextToken());
			number.set(Long.valueOf(st.nextToken()));
			context.write(number,word);
		}
	}
}
