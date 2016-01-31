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

	private static final String INPUT_FOLDER = "swc_input";
	private static final String OUTPUT_FOLDER_COUNT = "swc_output_count";
	private static final String OUTPUT_FOLDER_SORT = "swc_output_sort";
	
	private static final String OUTPUT_FILE = "part-r-00000";
	
	public static void main(String[] args) throws Exception {

		String file = FileHandler.copyFile(args[0], INPUT_FOLDER);
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
			FileInputFormat.addInputPath(jobCount, new Path(INPUT_FOLDER));
			FileOutputFormat.setOutputPath(jobCount, new Path(OUTPUT_FOLDER_COUNT));
			boolean success = jobCount.waitForCompletion(true);
			FileSystem fs = FileSystem.get(conf);
			fs.delete(new Path(INPUT_FOLDER), true);
			if(success){				
				Job jobOrder = Job.getInstance(conf, "word count");
				jobOrder.setJarByClass(SortedWordCount.class);
				jobOrder.setMapperClass(NumberMapper.class);
				jobOrder.setCombinerClass(RepeatReducer.class);
				jobOrder.setReducerClass(RepeatReducer.class);
				jobOrder.setOutputKeyClass(LongWritable.class);
				jobOrder.setOutputValueClass(Text.class);
				jobOrder.setSortComparatorClass(LongWritable.DecreasingComparator.class);
				FileInputFormat.addInputPath(jobOrder, new Path(OUTPUT_FOLDER_COUNT));
				FileOutputFormat.setOutputPath(jobOrder, new Path(OUTPUT_FOLDER_SORT));
				success = jobOrder.waitForCompletion(true);
				fs.delete(new Path(OUTPUT_FOLDER_COUNT), true);
				System.out.println(OUTPUT_FOLDER_SORT+System.getProperty("file.separator")+OUTPUT_FILE);
				FileHandler.uploadFile(OUTPUT_FOLDER_SORT+System.getProperty("file.separator")+OUTPUT_FILE, args[1]);
				fs.delete(new Path(OUTPUT_FOLDER_SORT), true);
			}
			System.exit(success ? 0 : 1);			
		}
	}
}
