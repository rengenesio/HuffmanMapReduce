package br.ufrj.ppgi.huffmanmapreduce.mapreduce.encoder;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import br.ufrj.ppgi.huffmanmapreduce.Codification;
import br.ufrj.ppgi.huffmanmapreduce.Defines;
import br.ufrj.ppgi.huffmanmapreduce.mapreduce.io.BytesWritableEncoder;

public class EncoderMap extends
		Mapper<LongWritable, BytesWritable, LongWritable, BytesWritableEncoder> {

	LongWritable key;
	int inc_key;
	boolean key_set;
	
	short symbols = 0;
	Codification[] codification;

	@Override
	protected void setup(
			Mapper<LongWritable, BytesWritable, LongWritable, BytesWritableEncoder>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		//this.key = new LongWritable(context.getTaskAttemptID().getTaskID().getId());
		this.inc_key = context.getNumReduceTasks();
		this.key_set = false;
		
		System.out.println("Inc_key: " + this.inc_key);
		
		codification = new Codification[Defines.POWER_BITS_CODIFICATION];
		symbols = 0;

		String file_cb = context.getConfiguration().get("file_cb");
		Path path = new Path(file_cb);
		FileSystem fs = FileSystem.get(new Configuration());
		FSDataInputStream f = fs.open(path);

		while (f.available() != 0) {
			byte symbol = (byte) f.read();
			byte size = (byte) f.read();
			byte[] code = new byte[(size & 0xFF)];

			f.read(code, 0, size & (0xFF));

			codification[symbols] = new Codification(symbol, size, new String(code));
			symbols++;
		}

		/*
		System.out.println("CODIFICATION: symbol (size) code"); 
		for(short i = 0 ; i < symbols ; i++)
			System.out.println(codification[i].toString());
		*/
	}

	public void map(LongWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		BytesWritableEncoder buffer = new BytesWritableEncoder(value.toString().length());
		
		if(this.key_set == false) {
			this.key = key;
			this.key_set = true;
			System.out.println("Key: " + this.key);
		}

		for (int i = 0; i < value.getLength(); i++) {
			for (short j = 0; j < symbols; j++) {
				if (codification[j].symbol == value.getBytes()[i]) {
					buffer.addCode(codification[j]);
					break;
				}
			}
		}

		context.write(this.key, buffer);
		this.key.set(this.key.get() + this.inc_key);
	}

	@Override
	protected void cleanup(
			Mapper<LongWritable, BytesWritable, LongWritable, BytesWritableEncoder>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}
	
	
}