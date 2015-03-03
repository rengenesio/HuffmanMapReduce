package br.ufrj.ppgi.huffmanmapreduce.mapreduce.encoder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import br.ufrj.ppgi.huffmanmapreduce.Defines;
import br.ufrj.ppgi.huffmanmapreduce.mapreduce.io.ByteInputFormat;
import br.ufrj.ppgi.huffmanmapreduce.mapreduce.io.BytesWritableEncoder;
import br.ufrj.ppgi.huffmanmapreduce.mapreduce.io.EncoderOutputFormat;

public class EncoderConfiguration extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		// When implementing tool
		Configuration conf = this.getConf();
		
		// Parse args
		String fileName = args[0];
		//int numReduces = Integer.parseInt(args[1]);

		// Create job
		Job job = Job.getInstance(conf, "HuffmanEncoder");
		job.setJarByClass(EncoderConfiguration.class);

		// Setup MapReduce job
		job.setMapperClass(EncoderMap.class);
		//job.setReducerClass(EncoderReduce.class);
		job.setNumReduceTasks(0);
		
		// Input
		FileInputFormat.addInputPath(job, new Path(fileName));
		job.setInputFormatClass(ByteInputFormat.class);
		
		// Specify key / value
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(BytesWritableEncoder.class);

		// Output
		FileOutputFormat.setOutputPath(job, new Path(fileName + Defines.pathSuffix + Defines.compressedSplitsPath));
		job.setOutputFormatClass(EncoderOutputFormat.class);
		
		// Configuration to be accessed by map classes
		conf.set("fileName", fileName);
		
		// Execute job and return status
		return job.waitForCompletion(false) ? 0 : 1;
	}
}