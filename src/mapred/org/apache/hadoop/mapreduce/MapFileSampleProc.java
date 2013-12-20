package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.DirUtil;
import org.apache.hadoop.mapreduce.EVStatistics.Stats;
import org.apache.hadoop.mapreduce.Job.FileStatusList;
import org.apache.hadoop.mapreduce.Job.OpType;
import org.apache.hadoop.mapreduce.Job.FileStatusList.FileComparator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SampleInputUtil;
import org.apache.hadoop.mapreduce.lib.input.SamplePath;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.mortbay.log.LOG;

/**
 * 
 * CombineSampleInputFormat process
 * the Key is a formated string "camera_id/time_stamp"
 * @author fan
 *
 */
public class MapFileSampleProc {
	public static final Log LOG = LogFactory.getLog(MapFileSampleProc.class);
	
	private Job originjob;
	private Configuration conf;
	private static Hashtable<String, List<SamplePath>> filereclist = new Hashtable<String, List<SamplePath>>();
	private long totalfilenum = 0;
	
	private int timeConstraint;
	private double errorConstraint;
	private double errorConstraintConfidence;
	private int initSampleRound;
	private int samplingPolicy; // 0: MH  1: uniform (proportion to folder)  2: same size per folder
	private boolean runGroundTruth;
	private int initSampleSize;
	private int sampleSizePerFolder;
	private int expCount;
	private float sample2ndRoundPctg;
	private DistributedFileSystem hdfs;
	private int max_slotnum;
	private long all_input_len = 0;
	private float sampleTimebudgetScale;
	
	
	private int start_index;
	private int end_index;
	private int filter_time_day_start; // e.g., 8 for 8am
	private int filter_time_day_end; // e.g., 18 for 6pm
	
	private String whiteList[];
	
	Random rand = new Random(); // It is better to share a global Random class.
	
	public static void resetWholeInputFileList() {
		filereclist.clear();
	}
	
	public boolean setup(Job job) throws ClassNotFoundException, IOException, InstantiationException, IllegalAccessException
	{
		originjob = job;
		hdfs = (DistributedFileSystem)(FileSystem.get(job.getConfiguration()));
		conf = job.getConfiguration();
		
		if (getInputFileRecordList() == false)
			return false;
		
		// Get time constraint.
		timeConstraint = job.getConfiguration().getInt("mapred.deadline.second", 200);
		// Get error constraint.
		//errorConstraint = job.getConfiguration().getFloat("mapred.deadline.error", (float) 1.0);
		// Get error constraint confidence.
		// NOTE that currently we only manually choose the confidence like 0.90 or 0.95. Change of this
		// value requires update of the std variance's coefficient in processReduceResults() in Job.java.
		//errorConstraintConfidence = job.getConfiguration().getFloat("mapred.deadline.errorConfidence",
		//		                                                    (float) 0.95);
		
		// Get trial/initial sample rounds number. Please set this to 1 at current stage (07/08/2013).
		//initSampleRound = job.getConfiguration().getInt("mapred.sample.initround", 1);
	    // Get trial/initial rounds sample unit size.
		//initSampleSize = job.getConfiguration().getInt("mapred.sample.initsize", 60);
		// Get sample size per folder for the 1st round.
		sampleSizePerFolder = job.getConfiguration().getInt("mapred.sample.sizePerFolder", 10);
		// Get sample time percentage for the 2nd round.
		sample2ndRoundPctg = job.getConfiguration().getFloat("mapred.sample.sampleTimePctg",
				                                                      (float) 0.3);
		  
		// Get the sampling policy (algorithm): 0: MaReV  1: uniform (proportion to folder)  2: same size per folder
		samplingPolicy = job.getConfiguration().getInt("mapred.sample.policy", 0);
		// Get whether this job is for ground truth with uniform sampling
		runGroundTruth = job.getConfiguration().getBoolean("mapred.sample.groundTruth", false);
		if (runGroundTruth) {
			samplingPolicy = 1;
			sample2ndRoundPctg = (float) 0.99;
		}
		expCount = job.getConfiguration().getInt("mapred.sample.experimentCount", 1);
		// Get number of Map slots in the cluster.
		sampleTimebudgetScale = job.getConfiguration().getFloat("mapred.sample.budgetScale", (float)1.0);		
		int datanode_num = hdfs.getDataNodeStats().length;
		int max_mapnum = job.getConfiguration().getInt("mapred.tasktracker.map.tasks.maximum", 2);
		max_slotnum = datanode_num * max_mapnum;
		LOG.info("datanode_num = " + datanode_num + "  max_mapnum = " + max_mapnum);
		
		if (max_slotnum <= 0) {
			return false;
		}
		return true;
	}	
	
	public boolean start() throws IOException, InterruptedException, ClassNotFoundException
	{
		/* start cache job first */
		//CacheJob cachejob = new CacheJob(originjob, filereclist);
		//cachejob.Start();
		
		List<SamplePath> files = GetWholeFileRecordList();
		
		int expC = 0;
		while (expC < expCount) {
			expC++;
			LOG.info("");
			LOG.info("*** number of images = " + files.size() + " ***");
			int runCount = 0;
			long deadline = System.currentTimeMillis() + timeConstraint * 1000; // in millisecond
			long timer = System.currentTimeMillis();
			long N = files.size(); // Total input records number.	
			if (runGroundTruth) {
				LOG.info("*** Sampling For GroundTruth ***");
			}
			// Clear any pre-existing stats, for example, from Caching setup job.
			originjob.clearEVStats();
			long extraCost = 0L;
			LOG.info("*** Deadline - " + timeConstraint + " ***");
			LOG.info("*** Policy - " + samplingPolicy + " ***");
			LOG.info("*** EXPERIMENT - " + expC + "/" + expCount + " ***");
			LOG.info("*** Sampling Round - " + runCount + " ***");	
			while(System.currentTimeMillis() < deadline)
			{		
				runCount += 1; 				
				LOG.debug("*** Deadline - " + timeConstraint + " ***");
				LOG.debug("*** Policy - " + samplingPolicy + " ***");
				LOG.debug("*** EXPERIMENT - " + expC + "/" + expCount + " ***");
				LOG.debug("*** Sampling Round - " + runCount + " ***");	
				
				long totalTimeCost = System.currentTimeMillis() - timer;
				timer = System.currentTimeMillis();				
				LOG.debug("To deadline: " + (deadline - System.currentTimeMillis()) + " ms");
				  
				// myStats format: Time average, value variance, Time count!
				Map<String, Stats> myStats = originjob.processEVStats();
				long avgTime = Long.valueOf(originjob.evStats.getAggreStat("time_per_record")); // in millisecond
				originjob.getConfiguration().set("mapred.sample.avgTimeCost", String.valueOf(avgTime));
				int totalSize = Integer.valueOf(originjob.evStats.getAggreStat("total_size")); 
				long firstMapperTime = Long.valueOf(originjob.evStats.getAggreStat("first_mapper_time")); // in millisecond
				long lastMapperTime = Long.valueOf(originjob.evStats.getAggreStat("last_mapper_time")); // in millisecond
				long avgReducerTime = Long.valueOf(originjob.evStats.getAggreStat("avg_reducer_time")); // in millisecond
					 
				long extraCostTemp = totalTimeCost - (lastMapperTime - firstMapperTime);
				// Note some rounds may be skipped, so totalTimeCost is small.
				if (extraCostTemp > 1000) { 
					extraCost = extraCostTemp;
				}
				LOG.debug("avgCost = " + avgTime + "ms   recordSize = " + totalSize +
						  "   extraCost = " + extraCost + "ms   totalCost = " + totalTimeCost + "ms");
				
				long time_budget = 0;
				  
				// get the files total size in a sample and determine the proper split size
				List<SamplePath> inputfiles = new ArrayList<SamplePath>();
				Long sample_len = 0L;
				Long sample_time = 0L;
				Long[] sample_results;
				
				if (runCount == 0) { // no sampling, all files
					for (SamplePath sp : files) {
						inputfiles.add(sp);					
					}
				} else if (runCount == 1) {
					sample_results = SamplingAlg.RandomSampleWithDirs(files, myStats,
							sampleSizePerFolder, inputfiles, filereclist, originjob);
					sample_len = sample_results[0];
					sample_time = sample_results[1];
				} else if (runCount == 2) {		
					  time_budget = (long) (((deadline - System.currentTimeMillis()) * sample2ndRoundPctg
							  				- extraCost) * sampleTimebudgetScale);
					  if (time_budget > 0) {
						  LOG.debug("Next sampleTime = " + time_budget + " ms (x " + max_slotnum + ")");					  
						  if (samplingPolicy == 0) { // MaReV 
							  sample_results = SamplingAlg.RandomSampleWithDirsByTime(files, myStats,
									  SamplingAlg.K_0_1, time_budget * max_slotnum, false, avgTime, inputfiles, filereclist, originjob);
							  sample_len = sample_results[0];
							  sample_time = sample_results[1];
						  } else if (samplingPolicy == 1) {  // Uniform 
							   sample_results = SamplingAlg.RandomSampleByTime(
									   files, myStats, time_budget * max_slotnum, avgTime, inputfiles, originjob);
							   sample_len = sample_results[0];
							   sample_time = sample_results[1];
						  } else if (samplingPolicy == 2) {  // Same per folder, same as MaReV
							  sample_results = SamplingAlg.RandomSampleWithDirsByTime(files, myStats,
									  SamplingAlg.K_0_1, time_budget * max_slotnum, false, avgTime, inputfiles, filereclist, originjob);
							  sample_len = sample_results[0];
							  sample_time = sample_results[1];
						 }					  
					  } else {
						  LOG.debug("Not enough time budget for Round-2, skipped!");
						  continue;
					  }
				  } else if (runCount >= 3) {
					  time_budget = (long) (((deadline - System.currentTimeMillis()) - extraCost) * sampleTimebudgetScale );
					  
					  if (time_budget > 0 ){
						  LOG.debug("Next sampleTime = " + time_budget + " ms (x " + max_slotnum + ")");	
						  if (samplingPolicy == 0) { // MaReV
							  sample_results = SamplingAlg.RandomSampleWithDistributionByTime(
									  files, myStats, time_budget * max_slotnum, false, inputfiles, filereclist, originjob);
						  	sample_len = sample_results[0];
						  	sample_time = sample_results[1];
						  } else if (samplingPolicy == 1) { // Uniform
							  sample_results = SamplingAlg.RandomSampleByTime(
									  files, myStats, time_budget * max_slotnum, avgTime, inputfiles, originjob);
							  sample_len = sample_results[0];
							  sample_time = sample_results[1];
						  } else if (samplingPolicy == 2) {  // Same per folder
							  sample_results = SamplingAlg.RandomSampleWithDirsByTime(files, myStats,
									  SamplingAlg.K_0_1, time_budget * max_slotnum, true, avgTime, inputfiles, filereclist, originjob);
							  sample_len = sample_results[0];
							  sample_time = sample_results[1];
						  }
					  } else {
						  LOG.debug("Not enough time budget for Round-" + runCount + ", skipped!");
						  break;
					  }
				  }
				
				Job newjob = new Job(originjob.getConfiguration(), "sample_" + expC + "_" + runCount);
				//LOG.info("Jar: " + newjob.getJar());
				FileOutputFormat.setOutputPath(newjob, 
						new Path(originjob.getConfiguration().get(("mapred.output.dir")) + "_" +  expC + "_" + runCount));
				
				/* set input file path */;
				inputfiles = GetReorderedInput(inputfiles);
				String[] inputarr = new String[inputfiles.size()];
				int i = 0;
				for (SamplePath sp : inputfiles)
				{
					inputarr[i] = sp.file_path + SampleInputUtil.DELIMITER + sp.sample_key + SampleInputUtil.DELIMITER + sp.size;
					i++;
				}
				String samp_input_str = StringUtils.join(inputarr, ",");
				newjob.getConfiguration().set(SampleInputUtil.SAMP_DIR, samp_input_str);
				LOG.debug("$$$$$$$$$ total sample num = " + inputfiles.size()
						+ ", sample length = " + sample_len + ", sample time = " + sample_time 
						+ ", max slot number = " + max_slotnum);
				
				// split by size, default max split size
				newjob.getConfiguration().setBoolean("mapred.input.fileinputformat.splitByTime", false);
				
				if (runCount == 1) { // split by size
					Long splitsize = (long) (sample_len / max_slotnum / 1.5);
					newjob.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", splitsize);
				} else { 
					if (!runGroundTruth) {  // split by time
						newjob.getConfiguration().setBoolean("mapred.input.fileinputformat.splitByTime", true);
						newjob.getConfiguration().setLong("mapred.input.fileinputformat.splitByTime.maxTime",
								(long)(sample_time / max_slotnum * 1.0));
						// Desired number of splits based on time.
						newjob.getConfiguration().setInt("mapreduce.input.fileinputformat.split.number", max_slotnum);
						// Add EVStats to Job configuration for splitting based on Time rather than size!
						addMapTimeCostToJob(newjob, myStats);
					}
				}
				newjob.waitForCompletion(true);
				  
				double[] results = originjob.processReduceResults(inputfiles.size(), N, OpType.SUM);
				LOG.debug("Result estimation: sum(avg(Loc)) = " + results[0] + "+-" + results[1] + 
						  " (95% confidence).\n");
			}
			long timeDiff = System.currentTimeMillis() - deadline;
			if (timeDiff >= 0)
				LOG.info("After deadline: " + Math.abs(timeDiff) + "ms");
			else
				LOG.info("Before deadline: " + Math.abs(timeDiff) + "ms");
			
			Map<String, Stats> myStats = originjob.processEVStats();
			long avgTime = Long.valueOf(originjob.evStats.getAggreStat("time_per_record")); // in millisecond
			int totalSize = Integer.valueOf(originjob.evStats.getAggreStat("total_size")); 
			long firstMapperTime = Long.valueOf(originjob.evStats.getAggreStat("first_mapper_time")); // in millisecond
			long lastMapperTime = Long.valueOf(originjob.evStats.getAggreStat("last_mapper_time")); // in millisecond
			long totalTimeCost = System.currentTimeMillis() - timer;
			long extraCostTemp = totalTimeCost - (lastMapperTime - firstMapperTime);
			// Note some rounds may be skipped, so totalTimeCost is small.
			if (extraCostTemp > 1000) { 
				extraCost = extraCostTemp;
			}
			LOG.debug("avgCost = " + avgTime + "ms   recordSize = " + totalSize +
					  "   extraCost = " + extraCost + "ms   totalCost = " + totalTimeCost + "ms");
			double[] results = originjob.processReduceResults(0, N, OpType.SUM);
			LOG.info("RESULT ESTIMATION: sum(avg(Loc)) = " + results[0] + "+-" + results[1] + 
					  " (95% confidence).");
		}
		
		return true;
	}
	
	/**
	 * Add time cost of each Key in EVStats into Job config
	 * TODO: for actual load balance
	 * @param job
	 * @param myStats
	 */
	private void addMapTimeCostToJob(Job job, Map<String, Stats> myStats) {
		for (String key : myStats.keySet()) {
			// e.g., "key = CMH085_L/1    avg(Time) = 283.7061855670103"
			String confKeyName = "mapred.input.fileinputformat.splitByTime." + key;
			job.getConfiguration().setLong(confKeyName, (long)(myStats.get(key).getAvg()));
		}		
	}

	/**
	 * Filer data with the timestamps by a certain time period of day.
	 * @return
	 */
	private boolean isValidTimeOfDay(String key) {
		String time = key.substring(key.lastIndexOf("/")+1);
		Date date = new java.util.Date(Long.parseLong(time));
		if(date.getHours() >= filter_time_day_start && date.getHours() < filter_time_day_end)
			return true;		
		else
			return false;
	}
	
	/**
	 * Filer data with the length
	 * @return
	 */
	private boolean isValidDataSize(long length) {
		if (length < 1000 || length > 200000) {
			return false;
		}
		return true;
	}
	
	/**
	 * Filer data with folder whitelist
	 * @return
	 */
	private boolean isWhiteList(String path, String[] whiteListArray) {
		for (String s: whiteListArray) {
			if(path.contains(s))
				return true;
		}
		return false;
	}
	
	/**
	 * Get whole SamplePath list
	 * @return
	 */
	private List<SamplePath> GetWholeFileRecordList()
	{
		List<SamplePath> files = new ArrayList<SamplePath>();
		for (List<SamplePath> lsp : filereclist.values())
		{
			files.addAll(lsp);
		}
		return files;
	}
	
	/**
	 * Put all record files in map to files into <\i>filereclist<\i>
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public boolean getInputFileRecordList() throws ClassNotFoundException, IOException, InstantiationException, IllegalAccessException
	{
		Path[] homeDirArr = FileInputFormat.getInputPaths(originjob);
		if (homeDirArr.length != 1)
		{
			LOG.warn("Illegle input directory number!!!");
			return false;
		}
		
		Path homeDir = homeDirArr[0];
		LOG.info("$$$$$$$$$$ Home dir = " + homeDir);
		FileStatus[] mapFiles = hdfs.listStatus(homeDir);
		
		start_index = originjob.getConfiguration().getInt("mapred.sample.startindex", 0);
		end_index = originjob.getConfiguration().getInt("mapred.sample.endindex", mapFiles.length-1);
		
		filter_time_day_start = originjob.getConfiguration().getInt("mapred.filter.startTimeOfDay", 0);
		filter_time_day_end = originjob.getConfiguration().getInt("mapred.filter.endTimeOfDay", 24);
		
		LOG.info("$$$$$$$$$$ mapFiles number = " + mapFiles.length);
		LOG.info("$$$$$$$$$$ start timeOfDay = " + filter_time_day_start);
		LOG.info("$$$$$$$$$$ end timeOfDay = " + filter_time_day_end);
		
		boolean enableWhiteList = originjob.getConfiguration().getBoolean("mapred.sample.enableWhiteList", false);
		String filter = originjob.getConfiguration().get("mapred.sample.whiteList", "");
		whiteList = filter.split(",");
		if (enableWhiteList) {
			LOG.info("$$$$$$$$$$ whiteList = " + filter);
		}
		
		// return if we have already set the input dataset
		if (filereclist.size() > 0){
			return true;
		}
		
		long wholeFileNum = 0, wholeFileSize = 0;
		long oriFileNum = 0, oriFileSize = 0;
		long whiteFileNum = 0, whiteFileSize = 0;
		long timeFileNum = 0, timeFileSize = 0;
		for (int index = start_index; index <= end_index; index++)
		{
			FileStatus mapfs = mapFiles[index];
			
			Path mappath = mapfs.getPath();
			@SuppressWarnings("resource")
			SequenceFile.Reader idxreader = new SequenceFile.Reader(hdfs, new Path(mappath, "index"), conf);
			List<SamplePath> reclist = new ArrayList<SamplePath>();;

			Text key = new Text();
		    LongWritable position = new LongWritable();
		    ArrayList<String> keylist = new ArrayList<String>();
		    ArrayList<Long> poslist = new ArrayList<Long>();
		    while(idxreader.next(key, position))
		    {
		    	keylist.add(key.toString());
		    	poslist.add(position.get());
		    }
		    
			String k;
			Long pos1 = poslist.get(0);
			Long pos2;
			long length;
			for (int i=0; i<keylist.size()-1; i++)
			{
				k = keylist.get(i);
				pos2 = poslist.get(i+1);
				length = pos2-pos1;
				pos1 = pos2;
				wholeFileNum++;
				if (length > 0)
					wholeFileSize += length;
				if (!isValidDataSize(length))			
					continue;
				oriFileNum++;
				oriFileSize += length;
				if (enableWhiteList && !isWhiteList(mappath.toString(), whiteList)) {
					continue;
				}
				SamplePath sp = new SamplePath(mappath, k, length);
				whiteFileNum++;
				whiteFileSize += length;
				// filter by time of day
				if (isValidTimeOfDay(k)) {
					reclist.add(sp);
					timeFileNum++;
					timeFileSize += length;
				}
			}
			if (reclist.size() > 0) {
				String folder = mappath.toString();
				folder = folder.substring(folder.lastIndexOf("/")+1);
				folder = folder + "/1";
				filereclist.put(folder, reclist);
			}
		}
		LOG.info("Total:    FileNum: " + wholeFileNum + "  FileSize: " + wholeFileSize + "(inaccurate)");
		LOG.info("Valid:    FileNum: " + oriFileNum + "  FileSize: " + oriFileSize);
		LOG.info("ByFolder: FileNum: " + whiteFileNum + "  FileSize: " + whiteFileSize);
		LOG.info("ByTime:   FileNum: " + timeFileNum + "  FileSize: " + timeFileSize);
		return true;
	}  
	
	/**
	 * Reorganize the input files
	 * @param input
	 * @return a map of (folder, sorted filename list)
	 */
	public Map<String, List<SamplePath>> ReorganizeInput(List<SamplePath> input)
	{
		Map<String, List<SamplePath>> retmap = new HashMap<String, List<SamplePath>>();
		for(SamplePath sp : input)
		{
			String folder = sp.file_path.toString();
			List<SamplePath> lsp = retmap.get(folder);
			if (lsp == null)
			{
				List<SamplePath> newfsl = new ArrayList<SamplePath>();
				newfsl.add(sp);
				retmap.put(folder, newfsl);
			}
			else
			{
				lsp.add(sp);
			}
		}
		for (List<SamplePath> value : retmap.values())
		{
			Collections.sort(value, new FileComparator());
		}
		return retmap;
	}
	  
	public class FileComparator implements Comparator<SamplePath>
	{
		public int compare(SamplePath f1, SamplePath f2)
		{
			String fs1 = f1.sample_key;
			String fs2 = f2.sample_key;
			long fn1 = Long.parseLong(fs1.substring(fs1.lastIndexOf("/")+1, fs1.length()));
			long fn2 = Long.parseLong(fs2.substring(fs2.lastIndexOf("/")+1, fs2.length()));
			long diff = fn1 - fn2;
			return (int)diff;
		}
	}

	public List<SamplePath> GetReorderedInput(List<SamplePath> input)
	{
		Map<String, List<SamplePath>> inputmap = ReorganizeInput(input);
		List<SamplePath> retlist = new ArrayList<SamplePath>();
		for (List<SamplePath> lsp : inputmap.values())
		{
			retlist.addAll(lsp);
		}
		return retlist;
	}
}
