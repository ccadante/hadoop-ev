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
	private Hashtable<String, List<SamplePath>> filereclist = new Hashtable<String, List<SamplePath>>();
	private long totalfilenum = 0;
	
	private int timeConstraint;
	private double errorConstraint;
	private double errorConstraintConfidence;
	private int initSampleRound;
	private int samplingPolicy; // 0: MH  1: uniform (proportion to folder)  2: same size per folder
	private int initSampleSize;
	private int sampleSizePerFolder;
	private float sample2ndRoundPctg;
	private DistributedFileSystem hdfs;
	private int max_slotnum;
	private long all_input_len = 0;
	private float sampleTimebudgetScale;
	
	
	private int start_index;
	private int end_index;
	private int filter_time_day_start; // e.g., 8 for 8am
	private int filter_time_day_end; // e.g., 18 for 6pm
	
	private String filterarr[];
	
	Random rand = new Random(); // It is better to share a global Random class.
	
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
		errorConstraint = job.getConfiguration().getFloat("mapred.deadline.error", (float) 1.0);
		// Get error constraint confidence.
		// NOTE that currently we only manually choose the confidence like 0.90 or 0.95. Change of this
		// value requires update of the std variance's coefficient in processReduceResults() in Job.java.
		errorConstraintConfidence = job.getConfiguration().getFloat("mapred.deadline.errorConfidence",
				                                                    (float) 0.95);
		
		// NOTE that we are using 3-rounds sampling as our bottomline workflow: 1st round to get time cost of 
		// one map; 2nd round to get the variance of variables; 3rd round to consume all the rest time.
		// In experiments, we may skip 1st round sometimes assuming we have prior knowledge of time cost of 
		// one map somehow (just for some test cases to get better time results)
		// 
		// Get trial/initial sample rounds number. Please set this to 1 at current stage (07/08/2013).
		initSampleRound = job.getConfiguration().getInt("mapred.sample.initround", 1);
	    // Get trial/initial rounds sample unit size.
		//initSampleSize = job.getConfiguration().getInt("mapred.sample.initsize", 60);
		// Get sample size per folder for the 1st round.
		sampleSizePerFolder = job.getConfiguration().getInt("mapred.sample.sizePerFolder", 10);
		// Get sample time percentage for the 2nd round.
		sample2ndRoundPctg = job.getConfiguration().getFloat("mapred.sample.sampleTimePctg",
				                                                      (float) 0.3);
		  
		// Get the sampling policy (algorithm): 0: MH  1: uniform (proportion to folder)  2: same size per folder
		samplingPolicy = job.getConfiguration().getInt("mapred.sample.policy", 0);
	
		// Get number of Map slots in the cluster.
		sampleTimebudgetScale = job.getConfiguration().getFloat("mapred.sample.budgetScale", (float)1.0);		
		int datanode_num = hdfs.getDataNodeStats().length;
		int max_mapnum = job.getConfiguration().getInt("mapred.tasktracker.map.tasks.maximum", 2);
		max_slotnum = datanode_num*max_mapnum;
		LOG.info("datanode_num=" + datanode_num + " max_mapnum=" + max_mapnum);
		//max_slotnum = job.getConfiguration().getInt("mapred.sample.slotnum", 20);
		if (max_slotnum <= 0) {
			return false;
		}
		return true;
	}	
	
	// NOTE that we are using 3-rounds sampling as our bottomline workflow: 1st round to get time cost of 
	  // one map; 2nd round to get the variance of variables; 3rd round to consume all the rest time.
	  // In experiments, we may skip 1st round sometimes assuming we have prior knowledge of time cost of 
	  // one map somehow (just for some test cases to get better time results)
	public boolean start() throws IOException, InterruptedException, ClassNotFoundException
	{
		/* start cache job first */
		//CacheJob cachejob = new CacheJob(originjob, filereclist);
		//cachejob.Start();
		
		// Clear any pre-existing stats, for example, from Caching setup job.
		originjob.clearEVStats();
		
		/* loop until deadline */
		List<SamplePath> files = GetWholeFileRecordList();
		LOG.info("*** number of images = " + files.size() + " ***");
		int runCount = 0;
		long deadline = System.currentTimeMillis() + timeConstraint * 1000; // in millisecond
		long timer = System.currentTimeMillis();
		long N = files.size(); // Total input records number.	
		while(System.currentTimeMillis() < deadline)
		{		
			runCount += 1; 
			LOG.info("");
			LOG.info("*** Sampling Round - " + runCount + " ***");
			long totalTimeCost = System.currentTimeMillis() - timer;
			timer = System.currentTimeMillis();
			long extraCost = 0;
			LOG.info("To deadline: " + (deadline - System.currentTimeMillis()) + " ms");
			  
			Map<String, Stats> myStats = null;
			// myStats: Time average, value variance, Time count
			myStats = originjob.processEVStats();
			  long avgTime = Long.valueOf(originjob.evStats.getAggreStat("time_per_record")); // in millisecond
			  int totalSize = Integer.valueOf(originjob.evStats.getAggreStat("total_size")); 
			  long firstMapperTime = Long.valueOf(originjob.evStats.getAggreStat("first_mapper_time")); // in millisecond
			  long lastMapperTime = Long.valueOf(originjob.evStats.getAggreStat("last_mapper_time")); // in millisecond
			  long avgReducerTime = Long.valueOf(originjob.evStats.getAggreStat("avg_reducer_time")); // in millisecond
			  // NOTE: when computing extraCost and nextSize, we need to consider the number of parallel
			  // Map slots.
			  if (avgTime > 0) {
				  //extraCost = totalTimeCost - avgTime * totalSize / max_slotnum; // in millisecond
				  extraCost = totalTimeCost - (lastMapperTime - firstMapperTime);
				  //nextSize = (int) (((deadline - System.currentTimeMillis()) * sample2ndRoundPctg - extraCost)
					//	  / avgTime * max_slotnum);
			  }
			  LOG.info("avgCost = " + avgTime + "ms ; recordSize = " + totalSize +
					  " ; extraCost = " + extraCost + "ms ; totalCost = " + totalTimeCost + "ms");
			  
			  long time_budget = (long) (((deadline - System.currentTimeMillis()) * sample2ndRoundPctg
					  				- extraCost) * sampleTimebudgetScale);
			  if (time_budget > 0) {
				  LOG.info("Next sampleSize = " + 0 + " (this may be inaccurate) sampleTime = " + 
						  	time_budget + " ms");
			  }
				  
			int nextSize = 1;
			// get the files total size in a sample and determine the proper split size
			List<SamplePath> inputfiles = new ArrayList<SamplePath>();
			Long sample_len = 0L;
			Long sample_time = 0L;
			Long[] sample_results;
			if (runCount >= 1) {
				sample_results = SamplingAlg.RandomSampleWithDirs(files, myStats,
						sampleSizePerFolder * runCount, inputfiles, filereclist, originjob);
				sample_len = sample_results[0];
				sample_time = sample_results[1];
			} else if (runCount == 2) {				
				  // NOTE: when computing extraCost and nextSize, we need to consider the number of parallel
				  // Map slots.
				  if (avgTime > 0) {
					  extraCost = totalTimeCost - (lastMapperTime - firstMapperTime);
					  nextSize = (int) (((deadline - System.currentTimeMillis()) * sample2ndRoundPctg - extraCost)
							  / avgTime * max_slotnum);
				  }
				  LOG.info("avgCost = " + avgTime + "ms ; recordSize = " + totalSize +
						  " ; extraCost = " + extraCost + "ms ; totalCost = " + totalTimeCost + "ms");
				  
				  time_budget = (long) (((deadline - System.currentTimeMillis()) * sample2ndRoundPctg
						  				- extraCost) * sampleTimebudgetScale);
				  if (time_budget > 0) {
					  LOG.info("Next sampleSize = " + nextSize + " (this may be inaccurate) sampleTime = " + 
							  	time_budget + " ms");					  
					  if (samplingPolicy == 0) { // MH 
						  sample_results = SamplingAlg.RandomSampleWithDistributionByTime(
								  files, myStats, time_budget, nextSize, true, inputfiles, originjob);
					  	  sample_len = sample_results[0];
					  	  sample_time = sample_results[1];
					  }
					  else if (samplingPolicy == 1) {  // Uniform 
//						   sample_len = RandomSampleByTime(files, distribution, time_budget, inputfiles);
//							  sample_len = RandomSample(files, (end_index-start_index+1)*sampleSizePerFolder, inputfiles);	//fy
//							  sample_number = originjob.getConfiguration().getInt("mapred.sample.number", 0);
						  sample_results = SamplingAlg.RandomSample(
								  files, myStats, filterarr.length*sampleSizePerFolder, inputfiles, originjob);	//fy
						  sample_len = sample_results[0];
					  	  sample_time = sample_results[1];
					  } else if (samplingPolicy == 2) {  // Same per folder
						  int sizePerFolder = (int) ((time_budget / avgTime) /  filereclist.keySet().size());
						  sample_results = SamplingAlg.RandomSampleWithDirs(
								  files, myStats, sizePerFolder, inputfiles, filereclist, originjob);
						  sample_len = sample_results[0];
					  	  sample_time = sample_results[1];
					 }					  
				  } else {
					  LOG.warn("Not enough time budget for Round-2, skipped!");
				  }
			  } else if (runCount >= 3) {				  
				  // NOTE: when computing extraCost and nextSize, we need to consider the number of parallel
				  // Map slots.
				  if (avgTime > 0) {
					  //extraCost = totalTimeCost - avgTime * totalSize / max_slotnum; // in millisecond
					  extraCost = totalTimeCost - (lastMapperTime - firstMapperTime);
					  nextSize = (int) ((deadline - System.currentTimeMillis() - extraCost)
							  / avgTime * max_slotnum);
				  }
				  LOG.info("avgCost = " + avgTime + "ms ; recordSize = " + totalSize +
						  " ; extraCost = " + extraCost + "ms ; totalCost = " + totalTimeCost + "ms");
				  time_budget = (long) (((deadline - System.currentTimeMillis()) - extraCost) * sampleTimebudgetScale );
				  if (time_budget > 0 ){
					  LOG.info("Next estimated sampleSize = " + nextSize + " sampleTime = " + 
							  	time_budget + " ms");
					  if (samplingPolicy == 0) { // MH
						  sample_results = SamplingAlg.RandomSampleWithDistributionByTime(
								  files, myStats, time_budget, nextSize, true, inputfiles, originjob);
					  	sample_len = sample_results[0];
					  	sample_time = sample_results[1];
					  } else if (samplingPolicy == 1) { // Uniform
						  sample_results = SamplingAlg.RandomSampleByTime(
								  files, myStats, time_budget, inputfiles, originjob);
						  sample_len = sample_results[0];
						  sample_time = sample_results[1];
					  } else if (samplingPolicy == 2) {  // Same per folder
						  int sizePerFolder = (int) ((time_budget / avgTime) /  filereclist.keySet().size());
						  sample_results = SamplingAlg.RandomSampleWithDirs(
								  files, myStats, sizePerFolder, inputfiles, filereclist, originjob);
						  sample_len = sample_results[0];
						  sample_time = sample_results[1];
					  }
				  } else {
					  LOG.warn("Not enough time budget for Round-" + runCount + ", skipped!");
				  }
			  }
			//LOG.info("Next sampleSize = " + nextSize);		
			if (nextSize <= 0) {
				LOG.info("Quit!");
				break;
			}
			
			Job newjob = new Job(originjob.getConfiguration(), "sample_" + runCount);
			LOG.info("Jar: " + newjob.getJar());
			LOG.info("Split minsize = " + FileInputFormat.getMinSplitSize(newjob));
			LOG.info("Split maxsize = " + FileInputFormat.getMaxSplitSize(newjob));
			FileOutputFormat.setOutputPath(newjob, 
					new Path(originjob.getConfiguration().get(("mapred.output.dir")) + "_" + runCount));
			
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
			LOG.info("$$$$$$$$$$$$$ total sample size = " + sample_len
					+ ", sample time = " + sample_time + ", max slot number = " + max_slotnum);
			
			if (runCount == 1) { // split by size
				newjob.getConfiguration().setBoolean("mapred.input.fileinputformat.splitByTime", false);
				Long splitsize = sample_len / max_slotnum / 2;
				newjob.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", splitsize);
			} else { // split by time
				newjob.getConfiguration().setBoolean("mapred.input.fileinputformat.splitByTime", true);
				newjob.getConfiguration().setLong("mapred.input.fileinputformat.splitByTime.maxTime",
						sample_time / max_slotnum);
				// Desired number of splits based on time.
				newjob.getConfiguration().setInt("mapreduce.input.fileinputformat.split.number", max_slotnum);
				// Add EVStats to Job configuration for splitting based on Time rather than size!
				addMapTimeCostToJob(newjob, myStats);
			}
			newjob.waitForCompletion(true);
			  
			double[] results = originjob.processReduceResults(inputfiles.size(), N, OpType.SUM);
			LOG.info("RESULT ESTIMATION: sum(avg(Loc)) = " + results[0] + "+-" + results[1] + 
					  " (95% confidence).\n");
		}
		long timeDiff = System.currentTimeMillis() - deadline;
		if (timeDiff >= 0)
			LOG.info("After deadline: " + Math.abs(timeDiff) + "ms");
		else
			LOG.info("Before deadline: " + Math.abs(timeDiff) + "ms");
		
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
	
	/*public boolean startWithErrorContraint() throws IOException, InterruptedException, ClassNotFoundException
	{		
		 start cache job first 
//		CacheJob cachejob = new CacheJob(originjob, filereclist);
//		cachejob.Start();
		
		// Clear any pre-existing stats, for example, from Caching setup job.
		originjob.clearEVStats();
				
		 loop until deadline 
		List<SamplePath> files = GetWholeFileRecordList();
		long N = files.size(); // Total input records size.		
		double error = Double.MAX_VALUE;
		double sampleEnlargeScale = 1.0; // The next sample size scale comparing to previous sample size.
		long preTimeBudget = 0;
		int runCount = 0;
		long timer = System.currentTimeMillis();
		while(error > errorConstraint)
		{		
			runCount++;
			LOG.info("*** Sampling Round - " + runCount + " ***");
			long totalTimeCost = System.currentTimeMillis() - timer;
			timer = System.currentTimeMillis();
			long extraCost = 0;
			LOG.info("Target error: " + errorConstraint + " (95%)\t Current error: " + error);
			  
			Map<String, Stats> distribution = null;
			int nextSize = 1;
			// get the files total size in a sample and determine the proper split size
			List<SamplePath> inputfiles = new ArrayList<SamplePath>();
			Long sample_len = 0L;
			if (runCount == 1) {
				//nextSize = runCount * initSampleSize; 
				sample_len = RandomSampleWithDirs(files, sampleSizePerFolder, inputfiles);
			} else if (runCount == 2) {
				  distribution = originjob.processEVStats();
				  long avgTime = Long.valueOf(originjob.evStats.getAggreStat("time_per_record")); // in millisecond
				  int totalSize = Integer.valueOf(originjob.evStats.getAggreStat("total_size")); 			  
				  // NOTE: when computing extraCost and nextSize, we need to consider the number of parallel
				  // Map slots.
				  if (avgTime > 0) {
					  extraCost = totalTimeCost - avgTime * totalSize / max_slotnum; // in millisecond
				  }
				  LOG.info("avgCost = " + avgTime + "ms  recordSize = " + totalSize +
						  "  extraCost = " + extraCost + "ms  totalCost = " + totalTimeCost + "ms");
				  // Enlarge time budget by 5 times at most in the 2nd round.
				  sampleEnlargeScale = Math.min(5.0, sampleEnlargeScale); 
				  long time_budget = (long) ((totalTimeCost - extraCost) * sampleEnlargeScale);
				  preTimeBudget = time_budget;
				  nextSize = (int) (time_budget / avgTime * max_slotnum);
				  if (time_budget > 0) {
					  LOG.info("Next estimated sampleSize = " + nextSize + " sampleTime = " + 
							  	time_budget + " ms  (sampleEnlargeScale = " + sampleEnlargeScale + ")");
					  AdjustDistribution(distribution); // We do not want full distribution, neither average distribution.
					  //sample_len = RandomSampleWithDistributionByTime(files, distribution, time_budget, nextSize,
					  if (samplingPolicy == 0) // MH
						  sample_len = RandomSampleWithDistributionByTime(files, distribution, time_budget, nextSize,	  											
								  		true, inputfiles);
					  else if (samplingPolicy == 1) { // Uniform
						   						  sample_len = RandomSampleByTime(files, distribution, time_budget, inputfiles);
						   					  } else if (samplingPolicy == 2) {  // Same per folder
						   						  int sizePerFolder = (int) ((time_budget / avgTime) /  filereclist.keySet().size());
						   						  sample_len = RandomSampleWithDirs(files, sizePerFolder, inputfiles);
						   					  }
					  //originjob.clearEVStatsSet(); // Clear EVStats ???
				  } else {
					  LOG.warn("Not enough time budget for Round-2, skipped!");
				  }
			  } else if (runCount >= 3) {
				  distribution = originjob.processEVStats();
				  long avgTime = Long.valueOf(originjob.evStats.getAggreStat("time_per_record")); // in millisecond
				  int totalSize = Integer.valueOf(originjob.evStats.getAggreStat("total_size")); 			  
				  // NOTE: when computing extraCost and nextSize, we need to consider the number of parallel
				  // Map slots.
				  if (avgTime > 0) {
					  extraCost = totalTimeCost - avgTime * totalSize / max_slotnum; // in millisecond
					  nextSize = 1;
				  }
				  LOG.info("avgCost = " + avgTime + "ms ; recordSize = " + totalSize +
						  " ; extraCost = " + extraCost + "ms ; totalCost = " + totalTimeCost + "ms");
				  
				  // Enlarge time budget by 10 times at most in the 3nd round or later.
				  sampleEnlargeScale = Math.min(10.0, sampleEnlargeScale); 
				  long time_budget = (long) (preTimeBudget  * sampleEnlargeScale);
				  preTimeBudget = time_budget;
				  nextSize = (int) (time_budget / avgTime * max_slotnum);
				  if (time_budget > 0 ){
					  LOG.info("Next estimated sampleSize = " + nextSize + " sampleTime = " + 
							  	time_budget + " ms  (sampleEnlargeScale = " + sampleEnlargeScale + ")");
					  //sample_len = RandomSampleWithDistributionByTime(files, distribution, time_budget, nextSize,
					  if (samplingPolicy == 0) // MH
						  sample_len = RandomSampleWithDistributionByTime(files, distribution, time_budget, nextSize,
								  	true, inputfiles);
					  
					  
					  else if (samplingPolicy == 1) 
					  { // Uniform
						sample_len = RandomSampleByTime(files, distribution, time_budget, inputfiles);
					  } else if (samplingPolicy == 2) 
					  {  // Same per folder
						   int sizePerFolder = (int) ((time_budget / avgTime) /  filereclist.keySet().size());
						   sample_len = RandomSampleWithDirs(files, sizePerFolder, inputfiles);
					  }
					  //originjob.clearEVStatsSet(); // Clear EVStats ???
				  } else {
					  LOG.warn("Not enough time budget for Round-" + runCount + ", skipped!");
				  }
			  }
			//LOG.info("Next sampleSize = " + nextSize);		
			if (nextSize <= 0) {
				LOG.info("Quit!");
				break;
			}
			
			if (distribution != null)
				sample_len = RandomSampleWithDistribution(files, distribution, nextSize, true, inputfiles);	
			else
				sample_len = RandomSampleWithDirs(files, nextSize, inputfiles);
			
			Job newjob = new Job(originjob.getConfiguration(), "sample_" + runCount);
			LOG.info(newjob.getJar());
			LOG.info("minsize = " + FileInputFormat.getMinSplitSize(newjob));
			LOG.info("maxsize = " + FileInputFormat.getMaxSplitSize(newjob));
			FileOutputFormat.setOutputPath(newjob, 
					new Path(originjob.getConfiguration().get(("mapred.output.dir")) + "_" + runCount));
			
			 set input filter 
			inputfiles = GetReorderedInput(inputfiles);
			String[] inputarr = new String[inputfiles.size()];
			int i = 0;
			for (SamplePath sp : inputfiles)
			{
				inputarr[i] = sp.file_path + SampleInputUtil.DELIMITER + sp.sample_key + SampleInputUtil.DELIMITER + sp.size;
				i++;
			}
			String samp_input_str = StringUtils.join(inputarr, ",");
			
			// all input files are included in newjob
			
			FileInputFormat.setInputPaths(newjob, new Path(inputfiles.get(0)));
			for (int j=1; j<inputfiles.size(); j++)
			{
				FileInputFormat.addInputPath(newjob, new Path(inputfiles.get(j)));
			}
			
			newjob.getConfiguration().set(SampleInputUtil.SAMP_DIR, samp_input_str);
			Long splitsize = sample_len/max_slotnum;
			newjob.getConfiguration().set("mapreduce.input.fileinputformat.split.maxsize", splitsize.toString());
			newjob.waitForCompletion(true);
			  
			double[] results = originjob.processReduceResults(inputfiles.size(), N, OpType.SUM);
			LOG.info("RESULT ESTIMATION: sum(avg(Loc)) = " + results[0] + "+-" + results[1] + 
					  " (95% confidence).\n");
			error = results[1];
			sampleEnlargeScale = Math.pow((error / errorConstraint), 2.0); // Update sampleEnlargeScale
		}
		LOG.info("*** Job is done! ***");
		
		return true;
	}
	*/
	
	// We adjust the distribution by adding every variance with the (max_var + min_var) / 2.0. 
	// This is to average the distribution a bit since we do not trust it completely. 
	  @SuppressWarnings("unused")
	private void AdjustDistribution(Map<String, Stats> distribution) {
		double minStd = Double.MAX_VALUE;
		double maxStd = Double.MIN_VALUE;
		for (String key : distribution.keySet()) {
			double std = distribution.get(key).getStd();
			if (std < minStd) {
				minStd = std;
			}
			if (std > maxStd) {
				maxStd = std;
			}
		}
		double adjustedStd = (maxStd + minStd) / 2.0;
		for (String key : distribution.keySet()) {
			distribution.get(key).setVar(Math.pow(distribution.get(key).getStd() + adjustedStd, 2));
		}
		for (String key : distribution.keySet()) {
	    	 LOG.info("(after adjustment) " +  key + "\tavg = "
	    			 + distribution.get(key).getAvg() + "  var = "
	    			 + distribution.get(key).getVar() + " count = "
	    			 + distribution.get(key).getCount());
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
		LOG.info("$$$$$$$$$$   home dir = " + homeDir);
		FileStatus[] mapFiles = hdfs.listStatus(homeDir);
		
		start_index = originjob.getConfiguration().getInt("mapred.sample.startindex", 0);
		end_index = originjob.getConfiguration().getInt("mapred.sample.endindex", mapFiles.length-1);
		
		filter_time_day_start = originjob.getConfiguration().getInt("mapred.filter.startTimeOfDay", 0);
		filter_time_day_end = originjob.getConfiguration().getInt("mapred.filter.endTimeOfDay", 24);
		
		LOG.info("$$$$$$$$$$ start index = " + start_index);
		LOG.info("$$$$$$$$$$ end index = " + end_index);
		LOG.info("$$$$$$$$$$ start timeOfDay = " + filter_time_day_start);
		LOG.info("$$$$$$$$$$ end timeOfDay = " + filter_time_day_end);
		
		String filter = originjob.getConfiguration().get("mapred.sample.filterlist", "");
		filterarr = filter.split(";");
	//	for (FileStatus mapfs : mapFiles)
		long oriFileNum = 0, oriFileSize = 0;
		long filterFileNum = 0, filterFileSize = 0;
		for (int index = start_index; index <= end_index; index++)
		{
			FileStatus mapfs = mapFiles[index];
			/*boolean hit = false;
			String pathstr = mapfs.getPath().toString();
			for(String str : filterarr)
			{
				if (str.equals(pathstr.substring(pathstr.lastIndexOf("/")+1)))
				{
					LOG.info("$$$$ File name = " + pathstr);
					hit = true;		
					break;
				}
			}
			
			if (hit == false)
				continue;*/
			
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
				if (length < 0) {
					continue;
				}
				SamplePath sp = new SamplePath(mappath, k, length);
				oriFileNum++;
				oriFileSize += length;
				// filter by time of day
				if (isValidTimeOfDay(k)) {
					reclist.add(sp);
					filterFileNum++;
					filterFileSize += length;
				}
			}
			filereclist.put(mappath.toString(), reclist);			
		}
		LOG.info("oriFileNum: " + oriFileNum + "  oriFileSize: " + oriFileSize);
		LOG.info("newFileNum: " + filterFileNum + "  newFileSize: " + filterFileSize);
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
