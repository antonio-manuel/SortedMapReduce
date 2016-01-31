package eu.antoniolopez.mapreduce.reducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RepeatReducer  extends Reducer<LongWritable,Text,LongWritable,Text> {
    
	private Text result = new Text();

    public void reduce(LongWritable key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      StringBuilder sb = new StringBuilder();
      String delim = "";
      for (Text val : values) {
          sb.append(delim).append(val);
          delim = ",";
      }
      
      result.set(sb.toString());
      context.write(key, result);
    }
  }