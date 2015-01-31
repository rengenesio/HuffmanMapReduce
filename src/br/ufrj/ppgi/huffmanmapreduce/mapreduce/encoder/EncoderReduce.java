package br.ufrj.ppgi.huffmanmapreduce.mapreduce.encoder;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import br.ufrj.ppgi.huffmanmapreduce.Codification;
import br.ufrj.ppgi.huffmanmapreduce.Defines;
import br.ufrj.ppgi.huffmanmapreduce.mapreduce.io.BytesWritableEncoder;

public class EncoderReduce extends Reducer<LongWritable, BytesWritableEncoder, LongWritable, BytesWritableEncoder> {
	private static final int INITIAL_CAPACITY = 100;
	private LongWritable k = new LongWritable(0);
	private BytesWritableEncoder bw;
	
	Codification eof;

	@Override
	protected void setup(
			Reducer<LongWritable, BytesWritableEncoder, LongWritable, BytesWritableEncoder>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		String file_cb = context.getConfiguration().get("file_cb");
		Path path = new Path(file_cb);
		FileSystem fs = FileSystem.get(new Configuration());
		FSDataInputStream f = fs.open(path);
	
		while (f.available() != 0) {
			byte symbol = (byte) f.read();
			byte size = (byte) f.read();
			byte[] code = new byte[(size & 0xFF)];
			
			if(symbol != Defines.EOF)
				f.skip(size & (0xFF));
			else {
				f.read(code, 0, size & (0xFF));
				eof = new Codification(symbol, size, new String(code));
			}
		}
		bw = new BytesWritableEncoder(INITIAL_CAPACITY);
	}

	public void reduce(LongWritable key, Iterable<BytesWritableEncoder> values,	Context context)
			throws IOException, InterruptedException {
		for (BytesWritableEncoder b : values)
			if(!bw.addBytesWritable(b)) {
				System.out.println("Capacidade maxima! Vou comecar um novo!");
				System.out.println(bw.length + " " + bw.bits);
				context.write(k, bw);
				k.set(k.get() + 1);
				bw.clean();
				bw.addBytesWritable(b);
			}
	}
	
	@Override
	protected void cleanup(
			Reducer<LongWritable, BytesWritableEncoder, LongWritable, BytesWritableEncoder>.Context context)
			throws IOException, InterruptedException {
		bw.addCode(eof);
		bw.complete = true;
		context.write(k, bw);
		super.cleanup(context);
	}
}