package br.ufrj.ppgi.huffmanmapreduce;

import java.io.IOException;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

import br.ufrj.ppgi.huffmanmapreduce.mapreduce.encoder.EncoderConfiguration;
import br.ufrj.ppgi.huffmanmapreduce.mapreduce.symbolcount.SymbolCountConfiguration;

public class Encoder {
	long[] frequency = new long[Defines.POWER_BITS_CODIFICATION];
	short symbols = 0;
	NodeArray nodeArray;
	Codification[] codification;

	public Encoder(String file_in, String path_out, String numReduces)
			throws Exception {
		String[] s = new String[3];
		s[0] = file_in;
		s[1] = path_out;
		s[2] = numReduces;
		
		// MAPREDUCE SYMBOL COUNT
		ToolRunner.run(new Configuration(), new SymbolCountConfiguration(), s);
		// END MAPREDUCE SYMBOL COUNT
		FileToFrequency(path_out);
		frequencyToNodeArray();
		huffmanEncode();
		treeToCode();
		codificationToHDFS(path_out);
		// MAPREDUCE SYMBOL ENCODER
		ToolRunner.run(new Configuration(), new EncoderConfiguration(), s);
		// END MAPREDUCE SYMBOL ENCODER
	}
	
	public void FileToFrequency(String path_out) throws IOException {
		Path path = new Path(path_out + "/symbolcount");
		FileSystem fs = FileSystem.get(new Configuration());
		FileStatus[] status = fs.listStatus(path);
		
		for(short i = 1 ; i < status.length ; i++) {
			FSDataInputStream f = fs.open(status[i].getPath());
			while(f.available() > 0) {
				int symbol = f.readInt();
				frequency[symbol] = f.readLong();
				symbols++;
			}
		}
		
		frequency[Defines.EOF] = 1;
		symbols++;
		
		
		System.out.println("FREQUENCY: symbol (frequency)");
		int sum = 0;
		for (int i = 0; i < frequency.length; i++)
			if (frequency[i] != 0) {
				System.out.println((int) i + "(" + frequency[i] + ")");
				sum += frequency[i];
			}
		System.out.println("\nTotal: " + sum);
		System.out.println("------------------------------");
		
		
	}
	
	public void frequencyToNodeArray() {
		nodeArray = new NodeArray((short) (symbols + 1));

		for (short i = 0; i < Defines.POWER_BITS_CODIFICATION; i++)
			if (frequency[i] > 0)
				nodeArray.insert(new Node((byte) i, frequency[i]));
		
		/*
		System.out.println(nodeArray.toString());
		*/
	}

	public void huffmanEncode() {
		while (nodeArray.size() > 1) {
			Node a, b, c;
			a = nodeArray.get(nodeArray.size() - 2);
			b = nodeArray.get(nodeArray.size() - 1);
			c = new Node((byte) 0, a.frequency + b.frequency, a, b);

			nodeArray.removeLastTwoNodes();
			nodeArray.insert(c);
			
			/*
			System.out.println(nodeArray.toString() + "\n");
			*/
		}
	}

	public void treeToCode() {
		Stack<Node> s = new Stack<Node>();
		codification = new Codification[symbols];
		Node n = nodeArray.get(0);
		short codes = 0;
		String path = new String();

		s.push(n);
		while (codes < symbols) {
			if (n.left != null) {
				if (!n.left.visited) {
					s.push(n);
					n.visited = true;
					n = n.left;
					path += "0";
				} else if (!n.right.visited) {
					s.push(n);
					n.visited = true;
					n = n.right;
					path += "1";
				} else {
					path = path.substring(0, path.length() - 1);
					n = s.pop();
				}
			} else {
				n.visited = true;
				codification[codes] = new Codification(n.symbol, path);
				path = path.substring(0, path.length() - 1);
				n = s.pop();
				codes++;
			}
		}

		/*
		System.out.println("CODIFICATION: symbol (size) code"); 
		for(short i = 0 ; i < symbols ; i++)
			System.out.println(codification[i].toString());
		*/
	}

	public void codificationToHDFS(String path_out) throws IOException {
		Path path = new Path(path_out + "/codification");
		FileSystem fs = FileSystem.get(new Configuration());
		FSDataOutputStream f = fs.create(path);
		
		for (short i = 0; i < symbols; i++)
			f.write(codification[i].toByteArray(), 0, codification[i].toByteArray().length);
		f.close();
	}
}
