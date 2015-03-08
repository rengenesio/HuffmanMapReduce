package br.ufrj.ppgi.huffmanmapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class Main {

	public static void main(String[] args) throws Exception {
		if(args.length < 3) { System.out.println("Falta(m) parametro(s)!"); return; }
		
		boolean encoder = false;
		boolean decoder = false;
		
		String fileName = args[0];
		int numReduces = Integer.parseInt(args[1]);
		switch(args[2]) {
		case "encoder":
			encoder = true;
			break;
			
		case "decoder":
			decoder = true;
			break;
			
		case "both":
			encoder = true;
			decoder = true;
			break;
		}
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		
		if(encoder) {
			long totalTime, startTime, endTime;
	
			try {
				fs.delete(new Path(fileName + Defines.pathSuffix + Defines.symbolCountSplitsPath), true);
				fs.delete(new Path(fileName + Defines.pathSuffix + Defines.compressedSplitsPath), true);
				fs.delete(new Path(fileName + Defines.pathSuffix + Defines.codificationFileName), true);
			} catch(Exception ex) { }
				
			startTime = System.nanoTime();
			new Encoder(fileName, numReduces);
			endTime = System.nanoTime();
			System.out.println("Compressão completa!");
			
			totalTime = endTime - startTime;
			System.out.println(totalTime/1000000000.0 + " s (encoder)");
		}

		if(decoder) {
			long totalTime, startTime, endTime;
			
			try {
				fs.delete(new Path(fileName + Defines.pathSuffix + "sequentialdecompressed"), true);
			} catch(Exception ex) { }
			
			startTime = System.nanoTime();
			new DecoderSequential(fileName + ".yarnmultithreadv2dir/" + Defines.compressedSplitsPath, fileName + Defines.pathSuffix + "sequentialdecompressed", fileName + Defines.pathSuffix + Defines.codificationFileName);
			endTime = System.nanoTime();
			System.out.println("Descompressão completa!");
				
			totalTime = endTime - startTime;
			System.out.println(totalTime/1000000000.0 + " s (decoder)");
		}
	}
}
