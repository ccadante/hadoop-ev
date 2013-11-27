package myorg.offline;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CombineOfflineMR{

	public static void main(String[] args) throws Exception {
//		System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
		
		Configuration conf = new Configuration();
//		conf.set("mapreduce.input.fileinputformat.split.maxsize", "16777216");
		DistributedCache.createSymlink(conf);
		DistributedCache.addCacheFile(new URI("hdfs://localhost:9000/libraries/libopencv_java244.so#libopencv_java244.so"), conf);
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: CombineOfflineMR <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "combile offline mapreduce");
		
		job.setJarByClass(CombineOfflineMR.class);
		job.setMapperClass(ImageCarCountMapper.class);
		job.setReducerClass(CarAggrReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		//job.setInputFormatClass(SequenceFileInputFilter.class);
		//SequenceFileInputFilter.setFilterClass(job, SequenceFileInputFilter.ListFilter.class);
		
		String[] inputs = otherArgs[0].split(",");
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		for (int i=0; i<inputs.length; i++)
		{
			FileInputFormat.addInputPath(job, new Path(inputs[i]+"/*"));
		}
//		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		//System.exit(job.waitForSampleCompletion() ? 0 : 1);
	}
	
}
