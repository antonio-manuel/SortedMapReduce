package eu.antoniolopez.mapreduce.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper  extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer st = new StringTokenizer(value.toString());
      while (st.hasMoreTokens()) {
        word.set(st.nextToken().replaceAll("[^\\p{L}\\p{Nd}]+", "").trim().toLowerCase());
        if(word.getLength()>0){
        	context.write(word, one);
        }
      }
    }
  }
