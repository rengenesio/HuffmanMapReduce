package br.ufrj.ppgi.huffmanmapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import br.ufrj.ppgi.huffmanmapreduce.mapreduce.decoder.DecoderConfiguration;

public class Decoder {
	Codification[] codification;
	short symbols = 0;
	byte max_code = 0;
	Path in, out, cb;
	byte[] codificationArrayElementSymbol;
	boolean[] codificationArrayElementUsed;

	public Decoder(String fileName, int numReduces)
			throws Exception {
		String[] s = new String[2];
		s[0] = fileName;
		s[1] = Integer.toString(numReduces);
		
		// MAPREDUCE SYMBOL COUNT
		ToolRunner.run(new Configuration(), new DecoderConfiguration(), s);
		// END MAPREDUCE SYMBOL COUNT

		huffmanDecode();
	}

	public void huffmanDecode() throws IOException {
		byte[] buffer = new byte[1];
		BitSet bufferBits = new BitSet();
		int index = 0;

		FileSystem fs = FileSystem.get(new Configuration());
		FileStatus[] status = fs.listStatus(in);
		FSDataOutputStream fout = fs.create(out);

		for (short i = 1; i < status.length; i++) {
			FSDataInputStream fin = fs.open(status[i].getPath());

			
		}
		fout.close();
	}
}
