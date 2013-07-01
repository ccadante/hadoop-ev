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

	// ***.seq/***.jpg
	public String getCacheKey()
	{
		return filename;
	}
	
	public long getLen()
	{
		return size;
	}
	// ***.seq/***.jpg
	public String getFileName()
	{
		return filename;
	}
	
	// hdfs://localhost:9000/...../***.seq
	public String getSeqName()
	{
		return seqname;
	}
	
	// ***.seq
	public String getReduceKey()
	{
		return filename.substring(0, filename.lastIndexOf("/"));
	}
}