package br.ufrj.ppgi.huffmanmapreduce.mapreduce.decoder;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import br.ufrj.ppgi.huffmanmapreduce.BitUtility;
import br.ufrj.ppgi.huffmanmapreduce.Codification;
import br.ufrj.ppgi.huffmanmapreduce.Defines;
import br.ufrj.ppgi.huffmanmapreduce.SerializationUtility;

public class DecoderMap extends
		Mapper<LongWritable, BytesWritable, LongWritable, Text> {

	LongWritable key;
	int inc_key;
	boolean key_set;
	
	short symbols = 0;
	Codification[] codificationArray = new Codification[Defines.twoPowerBitsCodification];
	
	byte max_code = 0;
	byte[] codificationArrayElementSymbol;
	boolean[] codificationArrayElementUsed;
	
	int lastCodificationArrayIndex = 0;
	
	@Override
	protected void setup(Mapper<LongWritable, BytesWritable, LongWritable, Text>.Context context) throws IOException, InterruptedException {
		super.setup(context);
		
		fileToCodification(context.getConfiguration());
		codeToTreeArray();
		
		this.key = new LongWritable(context.getTaskAttemptID().getTaskID().getId() * 256000000);
	}
	

	public void map(LongWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		
		byte[] bufferOutput = new byte[Defines.writeBufferSize];
		int bufferOutputIndex = 0;
		
		byte[] compressedByteArray = value.getBytes();
		int compressedBytesLengthInBits = value.getLength() * 8;
		int codificationArrayIndex = lastCodificationArrayIndex;
		for (int i = 0; i < compressedBytesLengthInBits ; i++) {
			codificationArrayIndex <<= 1;
			if (BitUtility.checkBit(compressedByteArray, i) == false)
				codificationArrayIndex += 1;
			else
				codificationArrayIndex += 2;

			if (codificationArrayElementUsed[codificationArrayIndex]) {
				if (codificationArrayElementSymbol[codificationArrayIndex] != 0) {
					bufferOutput[bufferOutputIndex++] = codificationArrayElementSymbol[codificationArrayIndex];
					
					if(bufferOutputIndex >= Defines.writeBufferSize) {
						context.write(this.key, new Text(bufferOutput));
						this.key.set(this.key.get() + 1);
						bufferOutputIndex = 0;
					}
					codificationArrayIndex = 0;
				} else {
					if(bufferOutputIndex > 0) {
						context.write(this.key, new Text(bufferOutput));
					}

					return;
				}
			}
		}
		
		if(bufferOutputIndex > 0) {
			context.write(this.key, new Text(bufferOutput));
		}
		
		this.lastCodificationArrayIndex = codificationArrayIndex;
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

	public void codeToTreeArray() {
		for(short i = 0 ; i < this.codificationArray.length ; i++) {
			this.max_code = (this.codificationArray[i].size > this.max_code) ? this.codificationArray[i].size : this.max_code;  
		}
		
		codificationArrayElementSymbol = new byte[(int) Math.pow(2, (max_code + 1))];
		codificationArrayElementUsed = new boolean[(int) Math.pow(2, (max_code + 1))];

		for (short i = 0; i < this.codificationArray.length; i++) {
			int index = 0;
			for (byte b : codificationArray[i].code) {
				index <<= 1;
				if (b == 0)
					index += 1;
				else
					index += 2;
			}
			codificationArrayElementSymbol[index] = codificationArray[i].symbol;
			codificationArrayElementUsed[index] = true;
		}

		/*
		System.out.println("codeToTreeArray():");
		System.out.println("TREE_ARRAY:"); 
		for(int i = 0 ; i < Math.pow(2,(max_code + 1)) ; i++) 
			if(codificationArrayElementUsed[i])
				System.out.println("i: " + i + " -> " + codificationArrayElementSymbol[i]);
		System.out.println("------------------------------");
		*/
	}
	
}