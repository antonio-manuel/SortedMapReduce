package eu.antoniolopez.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import eu.antoniolopez.mapreduce.io.FileHandler;
import eu.antoniolopez.mapreduce.io.Unziper;
import eu.antoniolopez.mapreduce.mapper.TokenizerMapper;
import eu.antoniolopez.mapreduce.reducer.IntSumReducer;

public class SortedWordCount {

	public static void main(String[] args) throws Exception {

		String file = FileHandler.copyFile(args[0], args[1]);
		if(file!=null){
			if(FileHandler.isZip(file)){
				Unziper.unzip(file);
			}

			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "word count");
			job.setJarByClass(SortedWordCount.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
}
