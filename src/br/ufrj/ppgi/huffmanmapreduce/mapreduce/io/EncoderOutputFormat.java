package br.ufrj.ppgi.huffmanmapreduce.mapreduce.io;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EncoderOutputFormat<K, V> extends FileOutputFormat<K, V> {

	protected static class EncoderRecordWriter<K, V> extends RecordWriter<K, V> {
		FSDataOutputStream out;

		public EncoderRecordWriter(FSDataOutputStream out) {
			this.out = out;
		}

		@Override
		public synchronized void write(K key, V value) throws IOException,
				InterruptedException {
			boolean test = value == null || value instanceof NullWritable;
			if (!test) {
				BytesWritableEncoder bw = (BytesWritableEncoder) value;
				if(!bw.complete)
					if (bw.index != bw.length)
						out.write(bw.getBytes(), 0, bw.getLength() - 1);
					else
						out.write(bw.getBytes(), 0, bw.getLength());
				else {
					out.write(bw.getBytes(), 0, bw.getLength());
				}
			}
		}

		@Override
		public synchronized void close(TaskAttemptContext context)
				throws IOException, InterruptedException {
			out.close();
		}
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		String extension = new String();
		Path file = getDefaultWorkFile(job, extension);
		FileSystem fs = file.getFileSystem(job.getConfiguration());
		FSDataOutputStream file_out = fs.create(file);
		return new EncoderRecordWriter<K, V>(file_out);
	}
}
