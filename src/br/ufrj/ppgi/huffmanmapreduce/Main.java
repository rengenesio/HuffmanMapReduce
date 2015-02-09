package br.ufrj.ppgi.huffmanmapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Main {

	public static void main(String[] args) throws Exception {
		long t, t1, t2;
		String file_in, path_out;
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		file_in = new String(args[0]);
		path_out = new String(file_in + ".mapreducedir");

//		try {
//			fs.delete(new Path(path_out), true);
//		} 
//		catch(Exception ex) {
//		}
		
		t1 = System.nanoTime();
		new Encoder(file_in, path_out, args[1]);
		t2 = System.nanoTime();
		t = t2 - t1;
		System.out.println(t/1000000000.0 + " s (encoder)");

		t1 = System.nanoTime();
		new Decoder(path_out);
		t2 = System.nanoTime();
		t = t2 - t1;
		System.out.println(t/1000000000.0 + " s (decoder)");
	}
}
