package org.apache.hadoop.mapreduce.lib.input;


import org.apache.hadoop.fs.Path;


public class SamplePath
{
	public Path file_path;
	public String sample_key;
	public long size;

	public SamplePath()
	{
		
	}
	
	public SamplePath(String f, String k, long s)
	{
		file_path = new Path(f);
		sample_key = k;
		size = s;
	}
	
	public SamplePath(Path p, String k, long s)
	{
		file_path = p;
		sample_key = k;
		size = s;
	}
}