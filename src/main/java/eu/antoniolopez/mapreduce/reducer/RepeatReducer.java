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

		for (Text val : values) {
			result.set(val);
			context.write(key, result);
		}

	}
}