package myorg.offline;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class CombineImageInputFormat extends CombineFileInputFormat<Text, BytesWritable> {

	@Override 
	protected boolean isSplitable(JobContext context, Path filename)
	{
		return true;
	}

	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) 
			throws IOException
	{
		CombineFileRecordReader<Text, BytesWritable> reader = new CombineFileRecordReader<Text, BytesWritable>(
				(CombineFileSplit)split, context, CombineImageRecordReader.class);
		return reader;
	}
}
