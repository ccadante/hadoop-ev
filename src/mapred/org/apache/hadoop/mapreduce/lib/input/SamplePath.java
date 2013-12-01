package org.apache.hadoop.mapreduce.lib.input;


import org.apache.hadoop.fs.Path;


public class SamplePath
{
	// Example: mappath: hdfs://temp-server:9000/user/temp/data10G-1/CCTV009a_L
	// 			key: CCTV009a_L/1/1373041287960
	// 			Length:50597
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