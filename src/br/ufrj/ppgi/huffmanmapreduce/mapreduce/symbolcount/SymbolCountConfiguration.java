package br.ufrj.ppgi.huffmanmapreduce.mapreduce.symbolcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import br.ufrj.ppgi.huffmanmapreduce.mapreduce.io.ByteCountOutputFormat;
import br.ufrj.ppgi.huffmanmapreduce.mapreduce.io.ByteInputFormat;

public class SymbolCountConfiguration extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		// When implementing tool
		Configuration conf = this.getConf();
		//args[0] = conf.get("fs.defaultFS").concat(args[0]);
		
		conf.set("yarn.app.mapreduce.am.log.level", "DEBUG");
		
		// Create job
		Job job = Job.getInstance(conf, "huffmanSymbolCount");
		job.setJarByClass(SymbolCountConfiguration.class);
		
		// Setup MapReduce job do not specify the number of Reducer
		job.setMapperClass(SymbolCountMap.class);
		job.setCombinerClass(SymbolCountReduce.class);
		job.setReducerClass(SymbolCountReduce.class);
	
		// Input
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(ByteInputFormat.class);
		
	
		// Specify key / value
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
	
		// Output
		FileOutputFormat.setOutputPath(job, new Path(args[0] + ".dir/symbolcount"));
		job.setOutputFormatClass(ByteCountOutputFormat.class);
		
		// Execute job and return status (false -> don't show messages)
		return job.waitForCompletion(false) ? 0 : 1;
	}
}