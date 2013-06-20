package org.apache.hadoop.mapreduce;


import java.io.IOException;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mortbay.log.Log;

public class CacheJob {
	
	public Job cachejob;
	public Hashtable<String, Integer> inputhash;
	
	public CacheJob(Job job, List<FileStatus> fslist) throws IOException, IllegalStateException, ClassNotFoundException
	{
		inputhash = new Hashtable<String, Integer>();
		for(FileStatus fs : fslist)
		{
			String filedir = fs.getPath().toString();
			inputhash.put(filedir, 1);
		}
		
		Configuration cacheconf = new Configuration();
		
		cachejob = new Job(cacheconf, job.getJobName() + "cache job");
		cachejob.setMapperClass(CacheJobMapper.class);
		cachejob.setReducerClass(job.getReducerClass());
		cachejob.setOutputKeyClass(job.getOutputKeyClass());
		cachejob.setOutputValueClass(job.getOutputValueClass());
		
	    FileInputFormat.setInputPaths(cachejob, new Path("/cache"));	    
	    String outputpath = FileOutputFormat.getOutputPath(job).toString();
	    outputpath = outputpath + "_cache";
	    FileOutputFormat.setOutputPath(cachejob, new Path(outputpath));
				
	}
	
	public void Start() throws IOException, InterruptedException, ClassNotFoundException
	{
		if (cachejob == null)
			Log.info("cache job null");
		else
			cachejob.waitForCompletion(true);
	}
	
	public class CacheJobMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] kvpair = line.split(";");
			if (inputhash.get(kvpair[0]) == 1)
			{
				System.out.println("CACHE HIT!!!   " + kvpair[0]);
				String keyout = kvpair[0].substring(0, kvpair[0].lastIndexOf("/"));
				context.write(new Text(keyout), new IntWritable(Integer.parseInt(kvpair[1])));
				System.out.println("CACHE HIT!!! key = " + key);
			}
		}
	}

}
