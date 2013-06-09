package myorg.offline;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class CombineImageRecordReader extends RecordReader<Text, BytesWritable> {

	private CombineFileSplit split;
	private Configuration conf;
	private Integer index;

	private final Text currKey = new Text();
	private final BytesWritable currValue = new BytesWritable();
	private boolean fileProcessed = false;
	
	public CombineImageRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException
	{
		this.split = split;
		this.conf = context.getConfiguration();
		this.index = index;
	}
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {	}
	
	@Override
	public void close() throws IOException { }
	
	@Override
	public float getProgress() throws IOException
	{
		return 0;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException
	{
		if (fileProcessed)
		{
			return false;
		}

		int fileLength = (int)split.getLength(index);
		byte[] result = new byte[fileLength];
		
		Path path = split.getPath(index);
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = null;
		try
		{
			String file_path = path.toString();
			int last_slash = file_path.lastIndexOf('/');
			String key_str = file_path.substring(0, last_slash);
			//currKey.set(key_str);
			currKey.set(file_path);
			
			in = fs.open(path);
			in = fs.open(path);
			IOUtils.readFully(in, result, 0, fileLength);
			currValue.set(result, 0, fileLength);
		}
		finally
		{
			IOUtils.closeStream(in);
		}
		this.fileProcessed = true;
		return true;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return currKey;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return currValue;
	}
}
