package org.apache.hadoop.mapreduce;

import org.apache.hadoop.mapred.DirUtil;


/**
 * 
 * A class indicating one record in a SequenceFile. I wanna make something like FileStatus
 * @author fan
 *
 */
public class SequenceFileRecord
{
	private String seqname;
	private String filename;
	private long size;
	
	public SequenceFileRecord(String seq, String file, long sz)
	{
		seqname = seq;
		filename = file;
		size = sz;
	}
	
	public String getCacheKey()
	{
		return filename;
	}
	
	public long getLen()
	{
		return size;
	}
	
	public String getFileName()
	{
		return filename;
	}
	
	public String getSeqName()
	{
		return seqname;
	}
}