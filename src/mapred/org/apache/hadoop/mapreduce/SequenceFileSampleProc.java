package org.apache.hadoop.mapreduce;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.DirUtil;
import org.apache.hadoop.mapreduce.EVStatistics.Stats;
import org.apache.hadoop.mapreduce.EVStatistics.StatsType;
import org.apache.hadoop.mapreduce.Job.OpType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 
 * SequenceFileInputFilter process
 * the Key is a formated string "camera_id/time_stamp"
 * @author fan
 *
 */
public class SequenceFileSampleProc {
	public static final Log LOG = LogFactory.getLog(SequenceFileSampleProc.class);
	
	private Job originjob;
	private HashMap<String, List<SequenceFileRecord>> keyreclist 
					= new HashMap<String, List<SequenceFileRecord>>();
	private long totalfilenum = 0;
	
	private int timeConstraint;
	private double errorConstraint;
	private double errorConstraintConfidence;
	private int initSampleRound;
	private int initSampleSize;
	private int sampleSizePerFolder;
	private float sample2ndRoundPctg;
	private DistributedFileSystem hdfs;
	private int max_slotnum;
	private long all_input_len = 0;
	
	Random rand = new Random(); // It is better to share a global Random class.
	
	public boolean setup(Job job) throws ClassNotFoundException, IOException, InstantiationException, IllegalAccessException
	{
		originjob = job;
		getInputFileRecordList();
		
		// Get time constraint.
		timeConstraint = job.getConfiguration().getInt("mapred.deadline.second", 150);
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
		initSampleSize = job.getConfiguration().getInt("mapred.sample.initsize", 60);
		// Get sample size per folder for the 1st round.
		sampleSizePerFolder = job.getConfiguration().getInt("mapred.sample.sizePerFolder", 10);
		// Get sample time percentage for the 2nd round.
		sample2ndRoundPctg = job.getConfiguration().getFloat("mapred.sample.sampleTimePctg",
				                                                      (float) 0.3);
		  
		// Get number of Map slots in the cluster.
		hdfs = (DistributedFileSystem)(FileSystem.get(job.getConfiguration()));
		
		int datanode_num = hdfs.getDataNodeStats().length;
		int max_mapnum = job.getConfiguration().getInt("mapred.tasktracker.map.tasks.maximum", 2);
		max_slotnum = datanode_num*max_mapnum;
		if (max_slotnum <= 0) {
			LOG.info("Can not read number of slots!  datanode_num=" + datanode_num +
					  " max_mapnum=" + max_mapnum);
			return false;
		}
		return true;
	}	
	
	public boolean start() throws IOException, InterruptedException, ClassNotFoundException
	{
		// Clear any pre-existing stats, for example, from Caching setup job.
		originjob.clearEVStats();
		
		/* start cache job first */
//		CacheJob cachejob = new CacheJob(originjob, keyreclist);
//		cachejob.Start();
		
		/* loop until deadline */
		List<SequenceFileRecord> files = GetWholeFileRecordList();
		int runCount = 0;
		long deadline = System.currentTimeMillis() + timeConstraint * 1000; // in millisecond
		long timer = System.currentTimeMillis();
		long N = files.size(); // Total input records size.		
		while(System.currentTimeMillis() < deadline)
		{		
			runCount++;
			LOG.info("*** Sampling Round - " + runCount + " ***");
			long totalTimeCost = System.currentTimeMillis() - timer;
			timer = System.currentTimeMillis();
			long extraCost = 0;
			LOG.info("To deadline: " + (deadline - System.currentTimeMillis()) + " ms");
			  
			Map<String, Stats> distribution = null;
			int nextSize = 1;
			// get the files total size in a sample and determine the proper split size
			List<SequenceFileRecord> inputfiles = new ArrayList<SequenceFileRecord>();
			Long sample_len = 0L;
			if (runCount == 1) {
				//nextSize = runCount * initSampleSize; 
				sample_len = RandomSampleWithDirs(files, sampleSizePerFolder, inputfiles);
			} else if (runCount == 2) {
				  distribution = originjob.processEVStats();
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
					  nextSize = (int) (((deadline - System.currentTimeMillis()) * sample2ndRoundPctg - extraCost)
							  / avgTime * max_slotnum);
				  }
				  LOG.info("avgCost = " + avgTime + "ms ; recordSize = " + totalSize +
						  " ; extraCost = " + extraCost + "ms ; totalCost = " + totalTimeCost + "ms");
				  
				  long time_budget = (long) ((deadline - System.currentTimeMillis()) * sample2ndRoundPctg
						  				- extraCost);
				  if (time_budget > 0) {
					  LOG.info("Next sampleSize = " + nextSize + " (this may be inaccurate) sampleTime = " + 
							  	time_budget + " ms");
					  AdjustDistribution(distribution); // We do not want full distribution, neither average distribution.
					  sample_len = RandomSampleWithDistributionByTime(files, distribution, time_budget, nextSize,
							  											true, inputfiles);
					  //originjob.clearEVStatsSet(); // Clear EVStats ???
				  } else {
					  LOG.warn("Not enough time budget for Round-2, skipped!");
				  }
			  } else if (runCount >= 3) {
				  distribution = originjob.processEVStats();
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
					  nextSize = (int) ((deadline - System.currentTimeMillis() - extraCost - avgReducerTime)
							  / avgTime * max_slotnum);
				  }
				  LOG.info("avgCost = " + avgTime + "ms ; recordSize = " + totalSize +
						  " ; extraCost = " + extraCost + "ms ; totalCost = " + totalTimeCost + "ms");
				  
				  long time_budget = (long) ((deadline - System.currentTimeMillis()) - extraCost);
				  if (time_budget > 0 ){
					  LOG.info("Next sampleSize = " + nextSize + " (this may be inaccurate) sampleTime = " + 
							  	time_budget + " ms");
					  sample_len = RandomSampleWithDistributionByTime(files, distribution, time_budget, nextSize,
							  											true, inputfiles);
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
			
			/*if (distribution != null)
				sample_len = RandomSampleWithDistribution(files, distribution, nextSize, true, inputfiles);	
			else
				sample_len = RandomSampleWithDirs(files, nextSize, inputfiles);*/
			Long splitsize = all_input_len/max_slotnum;
			LOG.info("max slot number = " + max_slotnum + "; split size = " + splitsize);
			
			Job newjob = new Job(originjob.getConfiguration(), "sample_" + runCount);
			LOG.info(newjob.getJar());
			newjob.getConfiguration().set("mapred.min.split.size", splitsize.toString());
			LOG.info("minsize = " + FileInputFormat.getMinSplitSize(newjob));
			LOG.info("maxsize = " + FileInputFormat.getMaxSplitSize(newjob));
			FileOutputFormat.setOutputPath(newjob, 
					new Path(originjob.getConfiguration().get(("mapred.output.dir")) + "_" + runCount));
			
			/* set input filter */
			List<String> inputstrs = new ArrayList<String>();
			for (SequenceFileRecord sfr : inputfiles)
				inputstrs.add(sfr.getCacheKey());
			SequenceFileInputFilter.ListFilter.setListFilter(newjob.getConfiguration(), inputstrs);
			
			// all input files are included in newjob
			/*
			FileInputFormat.setInputPaths(newjob, new Path(inputfiles.get(0)));
			for (int j=1; j<inputfiles.size(); j++)
			{
				FileInputFormat.addInputPath(newjob, new Path(inputfiles.get(j)));
			}
			*/
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
	
	
	public boolean startWithErrorContraint() throws IOException, InterruptedException, ClassNotFoundException
	{
		// Clear any pre-existing stats, for example, from Caching setup job.
		originjob.clearEVStats();
		int runCount = 0;
		long timer = System.currentTimeMillis();
		
		/* start cache job first */
//		CacheJob cachejob = new CacheJob(originjob, keyreclist);
//		cachejob.Start();
		
		/* loop until deadline */
		List<SequenceFileRecord> files = GetWholeFileRecordList();
		long N = files.size(); // Total input records size.		
		double error = Double.MAX_VALUE;
		double sampleEnlargeScale = 1.0; // The next sample size scale comparing to previous sample size.
		long preTimeBudget = 0;
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
			List<SequenceFileRecord> inputfiles = new ArrayList<SequenceFileRecord>();
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
				  LOG.info("avgCost = " + avgTime + "ms ; recordSize = " + totalSize +
						  " ; extraCost = " + extraCost + "ms ; totalCost = " + totalTimeCost + "ms");
				  // Enlarge time budget by 5 times at most in the 2nd round.
				  sampleEnlargeScale = Math.min(5.0, sampleEnlargeScale); 
				  long time_budget = (long) ((totalTimeCost - extraCost) * sampleEnlargeScale);
				  preTimeBudget = time_budget;
				  nextSize = (int) (time_budget / avgTime * max_slotnum);
				  if (time_budget > 0) {
					  LOG.info("Next sampleSize = " + nextSize + " (this may be inaccurate) sampleTime = " + 
							  	time_budget + " ms  (sampleEnlargeScale = " + sampleEnlargeScale + ")");
					  AdjustDistribution(distribution); // We do not want full distribution, neither average distribution.
					  sample_len = RandomSampleWithDistributionByTime(files, distribution, time_budget, nextSize,
							  											true, inputfiles);
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
					  LOG.info("Next sampleSize = " + nextSize + " (this may be inaccurate) sampleTime = " + 
							  	time_budget + " ms  (sampleEnlargeScale = " + sampleEnlargeScale + ")");
					  sample_len = RandomSampleWithDistributionByTime(files, distribution, time_budget, nextSize,
							  											true, inputfiles);
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
			
			/*if (distribution != null)
				sample_len = RandomSampleWithDistribution(files, distribution, nextSize, true, inputfiles);	
			else
				sample_len = RandomSampleWithDirs(files, nextSize, inputfiles);*/
			Long splitsize = all_input_len/max_slotnum;
			LOG.info("max slot number = " + max_slotnum + "; split size = " + splitsize);
			
			Job newjob = new Job(originjob.getConfiguration(), "sample_" + runCount);
			LOG.info(newjob.getJar());
			newjob.getConfiguration().set("mapred.min.split.size", splitsize.toString());
			LOG.info("minsize = " + FileInputFormat.getMinSplitSize(newjob));
			LOG.info("maxsize = " + FileInputFormat.getMaxSplitSize(newjob));
			FileOutputFormat.setOutputPath(newjob, 
					new Path(originjob.getConfiguration().get(("mapred.output.dir")) + "_" + runCount));
			
			/* set input filter */
			List<String> inputstrs = new ArrayList<String>();
			for (SequenceFileRecord sfr : inputfiles)
				inputstrs.add(sfr.getCacheKey());
			SequenceFileInputFilter.ListFilter.setListFilter(newjob.getConfiguration(), inputstrs);
			
			// all input files are included in newjob
			/*
			FileInputFormat.setInputPaths(newjob, new Path(inputfiles.get(0)));
			for (int j=1; j<inputfiles.size(); j++)
			{
				FileInputFormat.addInputPath(newjob, new Path(inputfiles.get(j)));
			}
			*/
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
	 * Random sample num files from a FileStatus List, before we get EVStats.
	 * @param files
	 * @param filePath[]
	 * @param num
	 * @param re_list
	 * @return the size in Bytes of all files in res_list
	 */
	private Long RandomSample(List<SequenceFileRecord> files, int num, List<SequenceFileRecord> res_list)
	{
		if (num > files.size())
			num = files.size();
		Long sample_len = new Long(0);
		for(int i=0; i<num; i++)
		{
			int idx = rand.nextInt(files.size()-1);
			res_list.add(files.get(idx));
			sample_len += files.get(idx).getLen();
		}
		return sample_len;
	}

	
	/**
	 * Get whole SequenceFileRecord list
	 * @return
	 */
	private List<SequenceFileRecord> GetWholeFileRecordList()
	{
		List<SequenceFileRecord> files = new ArrayList<SequenceFileRecord>();
		for (List<SequenceFileRecord> lsfr : keyreclist.values())
		{
			files.addAll(lsfr);
		}
		return files;
	}

	/**
	 * Get file list based on the proportion of different dimensions (i.e., location).
	 * @return
	 */
	private Long RandomSampleWithDistribution(List<SequenceFileRecord> files, Map<String, Stats> distribution,
		 int num, boolean useMHSampling, List<SequenceFileRecord> res_list)
	{
		if (distribution == null || distribution.size() == 0)
		{
			return RandomSample(files, num, res_list);
		}
		if (num > files.size())
			num = files.size();
		// Record the number of actual samples.
		Map<String, Double> sampledSize = new HashMap<String, Double>();
		// Get sample sizes from proportion distribution.
		Map<String, Double> sizeProportion = new HashMap<String, Double>();
		double total_std = 0;
		for (String key : distribution.keySet()) {
			sampledSize.put(key, 0.0);
			total_std += distribution.get(key).getStd();	
		}
	    for (String key : distribution.keySet()) {
			sizeProportion.put(key, num * distribution.get(key).getStd() / total_std);
			LOG.info("RandomSample-Proportion: " + key + " " + sizeProportion.get(key));
		}
		int count = num;
	    int failCount = 0;
		Random rand = new Random(); // This must be outside the loop.
		Long sample_len = new Long(0);
		String next_variable = ""; // The next should sampled variable by MH, x_i, e.g., camera-loc.
		while(count > 0.99)
		{
			int idx = rand.nextInt(files.size());
			SequenceFileRecord fileRec = files.get(idx);
			String folder = fileRec.getReduceKey();
			boolean isChosen = false;
			if (useMHSampling) { // MH sampling algorithm
				String cur_variable = folder;
				if (next_variable.equals("") || next_variable.equals(cur_variable)) {
					res_list.add(fileRec);
					sampledSize.put(cur_variable, sampledSize.get(cur_variable) + 1);
					sample_len += files.get(idx).getLen();
					count--;
					isChosen = true;
					// To find the next sample variable.
					int variable_idx = rand.nextInt(distribution.size());
					for (String key : distribution.keySet())
					{
						if (variable_idx == 0) 
						{
							next_variable = key;
							break;
						}
						variable_idx--;
					}
					// Determine the next vairable based on MH algorithm.
					next_variable = SamplingAlg.MHGetNextVariable(cur_variable, next_variable, distribution);
				}
				  
			} else { // Sample based on size proportion
				for (String key : sizeProportion.keySet()) {
					if (key.equals(folder) && sizeProportion.get(key) >= 1.0) {
						sizeProportion.put(key, sizeProportion.get(key) - 1.0); // decrease one from quota
						res_list.add(fileRec);
						sampledSize.put(key, sampledSize.get(key) + 1);					  
						sample_len += files.get(idx).getLen();
						count--;
						isChosen = true;
						break;
					}
				}
			}
			if (!isChosen)
				failCount++;
			else
				failCount = 0;
			// If we can not find valid samples after many tries, we accept it first if 
			// the folder (camera-loc) is of interest to us.
			if (failCount > 5 * num && failCount <= 5 * num ) {
				if (sizeProportion.containsKey(folder)) {
					sizeProportion.put(folder, sizeProportion.get(folder) - 1.0);
					res_list.add(fileRec);
					sample_len += files.get(idx).getLen();
					count--;
					failCount = 0;
				}
			} else if (failCount > 10 * num ) { // If failed too many times, just break.
				  break;
			}
		}
		for (String key : sampledSize.keySet()) {
			LOG.info("RandomSample-final: " + key + " " + sampledSize.get(key));
		}
		return sample_len;
	}
	 
	/**
	   * 
	   * @param files
	   * @param distribution
	   * @param time_total
	   * @param num_total This parameter is useless unless useMHSampling is false, in which case we sampling 
	   * 				  based on size proportion.
	   * @param useMHSampling
	   * @param res_list
	   * @return
	   */
	  private Long RandomSampleWithDistributionByTime(List<SequenceFileRecord> files,
			  Map<String, Stats> distribution,  long time_total, int num_total, boolean useMHSampling,
			  List<SequenceFileRecord> res_list)
	  {
		  if (distribution == null || distribution.size() == 0) {
			  return RandomSample(files, 0, res_list);
		  }
		  if (num_total > files.size())
			  num_total = files.size();
		  // Record the number of actual samples.
		  Map<String, Double> sampledSize = new HashMap<String, Double>();
		  // Get sample sizes from proportion distribution.
		  Map<String, Double> sizeProportion = new HashMap<String, Double>();
		  double total_std = 0;
		  for (String key : distribution.keySet()) {
			  sampledSize.put(key, 0.0);
			  total_std += distribution.get(key).getStd();		  
		  }
		  for (String key : distribution.keySet()) {
			  sizeProportion.put(key, num_total * distribution.get(key).getStd() / total_std);
			  LOG.info("RandomSample-Proportion: " + key + " " + sizeProportion.get(key));
		  }
			  
		  int count = num_total;
		  int time = 0;
		  int failCount = 0;
		  Random rand = new Random(); // This must be outside the loop.
		  Long sample_len = new Long(0);
		  String next_variable = ""; // The next should sampled variable by MH, x_i, e.g., camera-loc.
		  while(time < time_total && count > 0.99)
		  {
			  int idx = rand.nextInt(files.size());
			  SequenceFileRecord fileRec = files.get(idx);
			  String folder = fileRec.getReduceKey();
			  String cur_variable = folder;
			  boolean isChosen = false;
			  if (useMHSampling) { // MH sampling algorithm			  
				  if (distribution.containsKey(cur_variable) && 
						  (next_variable.equals("") || next_variable.equals(cur_variable))) {
					  res_list.add(fileRec);
					  sampledSize.put(cur_variable, sampledSize.get(cur_variable) + 1);	
					  sample_len += files.get(idx).getLen();
					  time += distribution.get(cur_variable).getAvg(); // add the time cost for this variable
					  count--;
					  isChosen = true;
					  // To find the next sample variable.
					  int variable_idx = rand.nextInt(distribution.size());
					  for (String key : distribution.keySet()) {
						  if (variable_idx == 0) {
							  next_variable = key;
							  break;
						  }
						  variable_idx--;
					  }
					  // Determine the next vairable based on MH algorithm.
					  next_variable = SamplingAlg.MHGetNextVariable(cur_variable, next_variable, distribution);
				  }
			  } else { // Sample based on size proportion
				  for (String key : sizeProportion.keySet()) {
					  if (key.equals(folder) && sizeProportion.get(key) >= 1.0) {
						  sizeProportion.put(key, sizeProportion.get(key) - 1.0); // decrease one from quota
						  res_list.add(fileRec);
						  sampledSize.put(key, sampledSize.get(key) + 1);					  
						  sample_len += files.get(idx).getLen();
						  time += distribution.get(cur_variable).getAvg(); // add the time cost for this variable
						  count--;
						  isChosen = true;
						  break;
					  }
				  }
			  }
			  if (!isChosen)
				  failCount++;
			  else
				  failCount = 0;
			  // If we can not find valid samples after many tries, we accept it first if 
			  // the folder (camera-loc) is of interest to us.
			  if (failCount > 5 * time_total && failCount <= 5 * time_total ) {
				  if (sizeProportion.containsKey(folder)) {
					  sizeProportion.put(folder, sizeProportion.get(folder) - 1.0);
					  res_list.add(fileRec);
					  sampledSize.put(folder, sampledSize.get(folder) + 1);
					  sample_len += files.get(idx).getLen();
					  time += distribution.get(folder).getAvg(); // add the time cost for this variable
					  count--;
					  failCount = 0;
				  }
			  } else if (failCount > 10 * time_total ) { // If failed too many times, just break.
				  break;
			  }
		  }
		  for (String key : sampledSize.keySet()) {
			  LOG.info("RandomSample-final: " + key + " " + sampledSize.get(key));
		  }
		  return sample_len;
	  }
	  
	  
	/**
	 * Sample with input Dirs 
	 * @param files
	 * @param numPerFolder
	 * @param res_list
	 * @return
	 */
	private Long RandomSampleWithDirs(List<SequenceFileRecord> files, int numPerFolder, List<SequenceFileRecord> res_list) {
		Map<String, Stats> sizeProportion = new HashMap<String, Stats>();
		for(int i=0; i<files.size(); i++)
		{
			String loc =files.get(i).getReduceKey();
			Stats newStats = originjob.evStats.new Stats();
			newStats.setVar(1.0);
			sizeProportion.put(loc, newStats); // average among directories.
		}
		int num = numPerFolder * sizeProportion.size(); // sum = #folder * numPerFolder.
		LOG.info("Next sampleSize = " + num);
		return RandomSampleWithDistribution(files, sizeProportion, num, false, res_list);
	}
	
	
	/**
	 * Put all record files in sequence to files into <\i>keyreclist<\i>
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public void getInputFileRecordList() throws ClassNotFoundException, IOException, InstantiationException, IllegalAccessException
	{
		Configuration conf = originjob.getConfiguration();
		InputFormat<?, ?> input = ReflectionUtils.newInstance(originjob.getInputFormatClass(), conf);
		List<FileStatus> seqfiles = ((FileInputFormat<?, ?>)input).getListStatus(originjob);
		for (FileStatus seqfs : seqfiles)
		{
			all_input_len += seqfs.getLen();
			Path seqpath = seqfs.getPath();
			SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), seqpath, conf);
			if (reader.isCompressed()) 
			{
				LOG.info("Values are compressed.");
	        }
            if (reader.isBlockCompressed()) 
            {
            	LOG.info("Records are block-compressed.");
            }
            List<SequenceFileRecord> sfrlist = new ArrayList<SequenceFileRecord>();
            Text key = (Text)reader.getKeyClass().newInstance();
            BytesWritable value = (BytesWritable)reader.getValueClass().newInstance();
            while(reader.next(key, value))
            {
            	long size = value.getLength();
            	SequenceFileRecord sfr = new SequenceFileRecord(seqpath.toString(), key.toString(), size);
            	sfrlist.add(sfr);
            	keyreclist.put(seqpath.toString(), sfrlist);
            	totalfilenum++;
            }
		}
		LOG.info("File number = " + totalfilenum);
	}  
	
	// Get the folder name from a full path name, which is the deepest directory name.
	private String GetFolderFromFullPath(String path) {
		String folder = null;
		try {
			folder = path;
			folder = folder.substring(0, folder.lastIndexOf("/"));
//			folder = folder.substring(folder.lastIndexOf("/")+1);
		} catch (Exception ex) {
			  LOG.info("GetFolderFromFullPath:" + ex.getMessage());
		}
		return folder;
	}
	  
	
}
