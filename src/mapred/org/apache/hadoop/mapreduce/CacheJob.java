package org.apache.hadoop.mapreduce;


import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.mortbay.log.Log;

public class CacheJob {
	
	public Job cachejob;
	
	@SuppressWarnings("deprecation")
	public CacheJob(Job job, List<FileStatus> fslist) throws IOException, IllegalStateException, ClassNotFoundException
	{
		JobConf jobconf = (JobConf)(job.getConfiguration());

		FileSystem hdfs = FileSystem.get(jobconf);
		String cache_prefix = job.getConfiguration().get("fs.default.name") + "/cache";
		
		if(!hdfs.exists(new Path(cache_prefix)))
			hdfs.mkdirs(new Path(cache_prefix));
		
		if(hdfs.exists(new Path(cache_prefix + "/inputfilelist")))
			hdfs.delete(new Path(cache_prefix + "/inputfilelist"));
		
		FSDataOutputStream os = hdfs.create(new Path(
						cache_prefix + "/inputfilelist"));
		for (FileStatus fs : fslist)
		{
			String line = fs.getPath().toString() + "\n";
			os.write(line.getBytes("UTF-8"));
		}
		
		JobConf cacheconf = new JobConf();
		
		cachejob = new Job(cacheconf, job.getJobName() + "cache job");
		Log.info("JAR NAME: " + jobconf.getJar());
		cacheconf.setJar(jobconf.getJar());
		cachejob.setMapperClass(CacheJobMapper.class);
		cachejob.setReducerClass(job.getReducerClass());
		cachejob.setOutputKeyClass(job.getOutputKeyClass());
		cachejob.setOutputValueClass(job.getOutputValueClass());
		cacheconf.setInt("mapred.evstatistic.enable", 0);
		
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
	
	public static class CacheJobMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
/*
		public Hashtable<String, Integer> inputhash = new Hashtable<String, Integer>();
		
		public void setup(Context context) throws IOException
		{
			Configuration jobconf = context.getConfiguration();
			FileSystem hdfs = FileSystem.get(jobconf);
			String inputfilelist = jobconf.get("fs.default.name") + "/cache/inputfilelist";
			FSDataInputStream ins = hdfs.open(new Path(inputfilelist));
			if (ins == null) return;
			String line = ins.readLine();
			while(line != null)
			{
				inputhash.put(line, 1);
				Log.info(line);
				line = ins.readLine();
			}
		}
*/		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			Hashtable<String, Integer> inputhash = new Hashtable<String, Integer>();
			Configuration jobconf = context.getConfiguration();
			FileSystem hdfs = FileSystem.get(jobconf);
			String inputfilelist = jobconf.get("fs.default.name") + "/cache/inputfilelist";
			FSDataInputStream ins = hdfs.open(new Path(inputfilelist));
			if (ins == null) return;
			String line = ins.readLine();
			while(line != null)
			{
				inputhash.put(line, 1);
//				Log.info(line);
				line = ins.readLine();
			}
			
			String valueline = value.toString();
			String[] kvpair = valueline.split(";");
			System.out.println("input hash size = " + inputhash.size());
			if (inputhash.get(kvpair[0]) != null)
			{
				System.out.println("CACHE HIT!!!   " + kvpair[0]);
				String keyout = kvpair[0].substring(0, kvpair[0].lastIndexOf("/"));
				context.write(new Text(keyout), new IntWritable(Integer.parseInt(kvpair[1])));
				System.out.println("CACHE HIT!!! key = " + key);
			}
		}
	}

}
