package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.mortbay.log.Log;



public class CombineSampleRecordReader extends RecordReader<Text, BytesWritable> {

	private CombineSampleSplit split;
	private Configuration conf;
	private Integer index = 0;
	
	private final Text currKey = new Text();
	private final BytesWritable currValue = new BytesWritable();
	private boolean fileProcessed = false;
	private FileSystem fs;
	
	private ArrayList<ReaderPathPair> readerpathlist = new ArrayList<ReaderPathPair>();
	
	public CombineSampleRecordReader(CombineSampleSplit split, TaskAttemptContext context, Integer index) throws IOException, Exception
	{
		this.initialize(split, context, index);
	}

	public CombineSampleRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
	{
		this.initialize(split, context);
	}
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException 
	{	
		this.split = (CombineSampleSplit)split;
		this.conf = context.getConfiguration();
		this.fs = FileSystem.get(conf);
	}
	
	public void initialize(InputSplit split, TaskAttemptContext context, Integer index) throws IOException, InterruptedException 
	{	
		this.split = (CombineSampleSplit)split;
		this.conf = context.getConfiguration();
		this.index = index;
		this.fs = FileSystem.get(conf);
	}
	
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
		Log.info("$$$$$ split path number = " + split.getNumPaths() + "; index = " + index);
		if (fileProcessed || index==split.getNumPaths())
		{
			return false;
		}

		SamplePath path = split.getPath(index);
		Path mapFilePath = path.file_path;
		String sampleKey = path.sample_key;
		long fileLength = path.size;		

		MapFile.Reader mfreader = getMapFileReader(mapFilePath.toString());
		BytesWritable value = new BytesWritable();
		mfreader.get(new Text(sampleKey), value);
		
		if (fileLength < 0 || fileLength > 100000)	return false;
		currKey.set(sampleKey);
		currValue.set(value.getBytes(), 0, (int) value.getLength() );
		index++;
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
	
	MapFile.Reader getMapFileReader(String path) throws IOException
	{
		for (ReaderPathPair rpp : readerpathlist)
		{
			if (rpp.filepath.equals(path))
				return rpp.mfreader;
		}
		MapFile.Reader mfreader = new MapFile.Reader(fs, path, conf);
		readerpathlist.add(new ReaderPathPair(mfreader, path));
		return mfreader;
	}
	
	public class ReaderPathPair
	{
		public MapFile.Reader mfreader;
		public String filepath;
		
		public ReaderPathPair(MapFile.Reader r, String p)
		{
			mfreader = r;
			filepath = p;
		}
	}
}

