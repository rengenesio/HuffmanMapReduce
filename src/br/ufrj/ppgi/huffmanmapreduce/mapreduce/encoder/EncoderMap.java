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
import br.ufrj.ppgi.huffmanmapreduce.SerializationUtility;
import br.ufrj.ppgi.huffmanmapreduce.mapreduce.io.BytesWritableEncoder;

public class EncoderMap extends
		Mapper<LongWritable, BytesWritable, LongWritable, BytesWritableEncoder> {

	LongWritable key;
	int inc_key;
	
	Codification[] codificationArray = new Codification[Defines.twoPowerBitsCodification];
	BytesWritableEncoder buffer = new BytesWritableEncoder(Defines.writeBufferSize);

	@Override
	protected void setup(
			Mapper<LongWritable, BytesWritable, LongWritable, BytesWritableEncoder>.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		
		this.key = new LongWritable(context.getTaskAttemptID().getTaskID().getId());
		this.inc_key = 1;
		fileToCodification(context.getConfiguration());
	}

	public void map(LongWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
	
		int valueLengthInBytes = value.getLength();
		for (int i = 0 ; i < valueLengthInBytes ; i++) {
			for (short j = 0; j < this.codificationArray.length; j++) {
				if (codificationArray[j].symbol == value.getBytes()[i]) {
					if (i < 10) {
						System.out.print((char) codificationArray[j].symbol);
					}
					if(buffer.addCode(codificationArray[j]) == false) {
						context.write(this.key, buffer);
						this.key.set(this.key.get() + this.inc_key);
						buffer.clean();
						buffer.addCode(codificationArray[j]);
					}
					break;
				}
			}
		}
	}

	@Override
	protected void cleanup(
			Mapper<LongWritable, BytesWritable, LongWritable, BytesWritableEncoder>.Context context)
			throws IOException, InterruptedException {
		super.cleanup(context);
	}
	
	
	public void fileToCodification(Configuration configuration) throws IOException {
		FileSystem fileSystem = FileSystem.get(configuration);
		FSDataInputStream inputStream = fileSystem.open(new Path(configuration.get("fileName") + Defines.pathSuffix + Defines.codificationFileName));

		byte[] byteArray = new byte[inputStream.available()];
		inputStream.readFully(byteArray);
		
		this.codificationArray = SerializationUtility.deserializeCodificationArray(byteArray);
		
		
		/*
		System.out.println("CODIFICATION: symbol (size) code"); 
		for(short i = 0 ; i < symbols ; i++)
			System.out.println(codificationArray[i].toString());
		*/
	}
	
}