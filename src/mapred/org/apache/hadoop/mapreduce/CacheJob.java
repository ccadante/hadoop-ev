package org.apache.hadoop.mapreduce;


import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.DirUtil;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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
		
		/* Get number of Map slots in the cluster. */
		DistributedFileSystem disthdfs = (DistributedFileSystem)(hdfs);
		int datanode_num = disthdfs.getDataNodeStats().length;
		int max_mapnum = jobconf.getInt("mapred.tasktracker.map.tasks.maximum", 2);
		int max_slotnum = datanode_num*max_mapnum;
		if (max_slotnum <= 0) {
			Log.info("Can not read number of slots!  datanode_num=" + datanode_num +
				  " max_mapnum=" + max_mapnum);
			return;
		}
		
		/* create an input list file in /cache */
		
		if(!hdfs.exists(new Path(cache_prefix)))
			hdfs.mkdirs(new Path(cache_prefix));
		
		if(hdfs.exists(new Path(cache_prefix + "/inputfilelist")))
			hdfs.delete(new Path(cache_prefix + "/inputfilelist"), true);
		
		FSDataOutputStream os = hdfs.create(new Path(
						cache_prefix + "/inputfilelist"));
		for (FileStatus fs : fslist)
		{
			int prelen = jobconf.get("fs.default.name").length();
			String line = fs.getPath().toString().substring(prelen) + "\n";
			os.write(line.getBytes("UTF-8"));
		}
		os.close();
		
		/* Calculate the files size in Byte */
		long input_len = hdfs.getContentSummary(new Path(cache_prefix + "/inputfilelist")).getLength();
		Long splitsize = input_len/max_slotnum;
		Log.info("Inputfilelist Length = " + input_len + "; Slot Num = " + max_slotnum + "; Split Size = " + splitsize);
		
		/* create the cache job */
		JobConf cacheconf = new JobConf();
		cacheconf.setJar(jobconf.getJar());
		cacheconf.setInt("mapred.evstatistic.enable", 0);
		cacheconf.setInt("mapred.evstats.serverport", jobconf.getInt("mapred.evstats.serverport", 0));
		
		cachejob = new Job(cacheconf, job.getJobName() + "cache job");
		Log.info("JAR NAME: " + jobconf.getJar());
		cachejob.setMapperClass(CacheJobMapper.class);
		cachejob.setReducerClass(job.getReducerClass());
		cachejob.setInputFormatClass(TextInputFormat.class);
		cachejob.setOutputKeyClass(job.getOutputKeyClass());
		cachejob.setOutputValueClass(job.getOutputValueClass());
		cachejob.getConfiguration().set("mapreduce.input.fileinputformat.split.maxsize", splitsize.toString());
		
	    FileInputFormat.setInputPaths(cachejob, new Path("/cache/inputfilelist"));	    
	    String outputpath = FileOutputFormat.getOutputPath(job).toString();
	    outputpath = outputpath + "_cache";
	    FileOutputFormat.setOutputPath(cachejob, new Path(outputpath));
	}
	
	public void Start() throws IOException, InterruptedException, ClassNotFoundException
	{
		FileSystem hdfs = FileSystem.get(cachejob.getConfiguration());
		String cache_prefix = cachejob.getConfiguration().get("fs.default.name") + "/cache";
		if (cachejob == null)
			Log.info("cache job null");
		else
		{
			cachejob.waitForCompletion(true);
			Log.info("REDCLASS: " + cachejob.getReducerClass().toString());
			if(hdfs.exists(new Path(cache_prefix + "/inputfilelist")))
				hdfs.delete(new Path(cache_prefix + "/inputfilelist"), true);
		}
	}
	
	public static class CacheJobMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

		public Hashtable<String, Integer> cachehash = new Hashtable<String, Integer>();
		
		public void setup(Context context) throws IOException
		{
			Configuration jobconf = context.getConfiguration();
			FileSystem hdfs = FileSystem.get(jobconf);
			String cache_prefix = "/cache";
			FileStatus cachefiles[] = hdfs.listStatus(new Path(cache_prefix));
			Log.info("Cache files number = " + cachefiles.length);
			for(int i = 0; i< cachefiles.length; i++)
			{
				Path cachefile = cachefiles[i].getPath();
				// ignore inputfilelist
				if (DirUtil.GetLastSeg(cachefile.toString()).equals("inputfilelist"))
					continue;
				FSDataInputStream ins = hdfs.open(cachefile);
				BufferedReader reader = new BufferedReader(new InputStreamReader(ins));
				System.out.println("input file list: " + cachefile);
				if (ins == null) 
				{
					System.out.println("Cannot open input file list");
					return;
				}
				String line = reader.readLine();
				while(line != null)
				{
					String[] kvpair = line.split(";");
					cachehash.put(kvpair[0], Integer.parseInt(kvpair[1]));
					System.out.println("			" + line);
					line = reader.readLine();
				}
				reader.close();
				ins.close();
			}
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			Configuration jobconf = context.getConfiguration();
			String valueline = value.toString();
			System.out.println("input file = " + valueline + "; hashsize = " + cachehash.size());
			Integer cacheresult = cachehash.get(valueline);
			if (cacheresult != null)
			{
				System.out.println("CACHE HIT!!!   " + valueline);
				String keyout = valueline.substring(0, valueline.lastIndexOf("/"));
				context.write(new Text(keyout), new IntWritable(cacheresult));
				System.out.println("CACHE HIT!!! key = " + key);
			}
		}
	}
	
	

}
