package br.ufrj.ppgi.huffmanmapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Main {

	public static void main(String[] args) throws Exception {
		long t, t1, t2;
		String in, out, cb;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		in = new String(args[0]);
		out = new String(in);
		cb = new String(in);
		out += ".dir/compressed";
		cb += ".dir/codification";

		fs.delete(new Path(args[0] + ".dir"), true);
		
		t1 = System.nanoTime();
		new Encoder(in, out, cb, args[1]);
		t2 = System.nanoTime();
		t = t2 - t1;
		System.out.println(t/1000000000.0 + " s (encoder)");

		//in += ".dir/decompressed";
		//t1 = System.nanoTime();
		//new Decoder(out, in, cb);
		//t2 = System.nanoTime();
		//t = t2 - t1;
		//System.out.println(t/1000000000.0 + " s (decoder)");
	}
}
