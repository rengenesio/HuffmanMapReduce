package br.ufrj.ppgi.huffmanmapreduce.mapreduce.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;

public class BlockRecordReader extends
		RecordReader<LongWritable, BytesWritable> {
 
	private long start;
	private long length;
	private long bufferLength;
	private long pos;
	private long end;
	private SplitLineReader in;
	private FSDataInputStream fileIn;
	private LongWritable key;
	private BytesWritable value;

	public BlockRecordReader() {
	}

	@Override
	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit split = (FileSplit) genericSplit;
		
		System.out.println(split.toString());
		
		Configuration job = context.getConfiguration();

		this.start = split.getStart();
		this.length = split.getLength();
		this.bufferLength = this.length;
		this.end = this.start + this.length;
		this.pos = start;

		final Path file = split.getPath();
		final FileSystem fs = file.getFileSystem(job);
		fileIn = fs.open(file);

		this.key = new LongWritable();
		this.value = new BytesWritable();
	}

	private long getFilePosition() throws IOException {
		return pos;
	}

	@Override
	public boolean nextKeyValue() throws IOException {
		byte[] b = null;
		int bytes;

		if (bufferLength <= 0)
			return false;

		boolean alloc_ok = false;
		while (!alloc_ok) {
			try {
				b = new byte[(int) bufferLength];
			} catch (OutOfMemoryError e) {
				bufferLength /= 2;
				continue;
			}
			alloc_ok = true;
		}

		fileIn.seek(this.pos);
		bytes = fileIn.read(b, 0, (int) bufferLength);
		this.length -= bytes;

		this.key.set(this.pos);
		this.value.set(b, 0, bytes);
		this.pos += bytes;
		return true;
	}

	@Override
	public LongWritable getCurrentKey() {
		return this.key;
	}

	@Override
	public BytesWritable getCurrentValue() {
		return this.value;
	}

	@Override
	public float getProgress() throws IOException {
		if (start == end)
			return 0.0f;
		else
			return Math.min(1.0f, (getFilePosition() - start) / (float) (end - start));
	}

	@Override
	public void close() throws IOException {
		if (in != null)
			in.close();
	}

}
