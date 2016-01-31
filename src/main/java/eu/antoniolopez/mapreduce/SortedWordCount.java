package eu.antoniolopez.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import eu.antoniolopez.mapreduce.io.FileHandler;
import eu.antoniolopez.mapreduce.io.Unziper;
import eu.antoniolopez.mapreduce.mapper.TokenizerMapper;
import eu.antoniolopez.mapreduce.reducer.IntSumReducer;

import eu.antoniolopez.mapreduce.mapper.NumberMapper;
import eu.antoniolopez.mapreduce.reducer.RepeatReducer;

public class SortedWordCount {

	public static void main(String[] args) throws Exception {

		String file = FileHandler.copyFile(args[0], args[1]);
		if(file!=null){
			if(FileHandler.isZip(file)){
				Unziper.unzip(file);
			}

			Configuration conf = new Configuration();
			Job jobCount = Job.getInstance(conf, "word count");
			jobCount.setJarByClass(SortedWordCount.class);
			jobCount.setMapperClass(TokenizerMapper.class);
			jobCount.setCombinerClass(IntSumReducer.class);
			jobCount.setReducerClass(IntSumReducer.class);
			jobCount.setOutputKeyClass(Text.class);
			jobCount.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(jobCount, new Path(args[1]));
			FileOutputFormat.setOutputPath(jobCount, new Path(args[2]));
			boolean success = jobCount.waitForCompletion(true);
			FileSystem fs = FileSystem.get(conf);
			fs.delete(new Path(args[1]), true);
			if(success){				
				Job jobOrder = Job.getInstance(conf, "word count");
				jobOrder.setJarByClass(SortedWordCount.class);
				jobOrder.setMapperClass(NumberMapper.class);
				jobOrder.setCombinerClass(RepeatReducer.class);
				jobOrder.setReducerClass(RepeatReducer.class);
				jobOrder.setOutputKeyClass(LongWritable.class);
				jobOrder.setOutputValueClass(Text.class);
				jobOrder.setSortComparatorClass(LongWritable.DecreasingComparator.class);
				FileInputFormat.addInputPath(jobOrder, new Path(args[2]));
				FileOutputFormat.setOutputPath(jobOrder, new Path(args[3]));
				success = jobOrder.waitForCompletion(true);
				fs.delete(new Path(args[2]), true);
			}
			System.exit(success ? 0 : 1);			
		}
	}
}
