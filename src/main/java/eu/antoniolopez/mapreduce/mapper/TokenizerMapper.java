package eu.antoniolopez.mapreduce.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper  extends Mapper<Object, Text, Text, IntWritable>{


	private static final String REGEXP_CHARS_NUMS = "[^\\p{L}\\p{Nd}]+";

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException {
		StringTokenizer st = new StringTokenizer(value.toString());
		while (st.hasMoreTokens()) {
			word.set(st.nextToken().replaceAll(REGEXP_CHARS_NUMS, "").trim().toLowerCase());
			if(word.getLength()>0){
				context.write(word, one);
			}
		}
	}
}
