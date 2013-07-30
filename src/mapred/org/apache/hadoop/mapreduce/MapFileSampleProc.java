package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
	private Hashtable<String, List<SamplePath>> filereclist 
					= new Hashtable<String, List<SamplePath>>();
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
	private String filterarr[];
	private int sample_number;
	private int sample_dis;
	
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
		initSampleSize = job.getConfiguration().getInt("mapred.sample.initsize", 60);
		// Get sample size per folder for the 1st round.
		sampleSizePerFolder = job.getConfiguration().getInt("mapred.sample.sizePerFolder", 10);
		// Get sample time percentage for the 2nd round.
		sample2ndRoundPctg = job.getConfiguration().getFloat("mapred.sample.sampleTimePctg",
				                                                      (float) 0.3);
		  
		// Get the sampling policy (algorithm): 0: MH  1: uniform (proportion to folder)  2: same size per folder
		samplingPolicy = job.getConfiguration().getInt("mapred.sample.policy", 0);
	
		// Get number of Map slots in the cluster.
		sampleTimebudgetScale = job.getConfiguration().getFloat("mapred.sample.budgetScale", (float)1.8);		
		int datanode_num = hdfs.getDataNodeStats().length;
		int max_mapnum = job.getConfiguration().getInt("mapred.tasktracker.map.tasks.maximum", 2);
		//max_slotnum = datanode_num*max_mapnum;
		//LOG.info("Can not read number of slots!  datanode_num=" + datanode_num +
					  //" max_mapnum=" + max_mapnum);
		max_slotnum = job.getConfiguration().getInt("mapred.sample.slotnum", 46);
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
		
		// Clear any pre-existing stats, for example, from Caching setup job.
		originjob.clearEVStats();
		
		/* loop until deadline */
		List<SamplePath> files = GetWholeFileRecordList();
		LOG.info("*** number of images = " + files.size() + " ***");
		int runCount = -1;
		long deadline = System.currentTimeMillis() + timeConstraint * 1000; // in millisecond
		long timer = System.currentTimeMillis();
		long N = files.size(); // Total input records number.	
		while(System.currentTimeMillis() < deadline)
		{		
			runCount+=2;
			LOG.info("*** Sampling Round - " + runCount + " ***");
			long totalTimeCost = System.currentTimeMillis() - timer;
			timer = System.currentTimeMillis();
			long extraCost = 0;
			LOG.info("To deadline: " + (deadline - System.currentTimeMillis()) + " ms");
			  
			Map<String, Stats> distribution = null;
			int nextSize = 1;
			// get the files total size in a sample and determine the proper split size
			List<SamplePath> inputfiles = new ArrayList<SamplePath>();
			Long sample_len = 0L;
			if (runCount == 1) {
				//nextSize = runCount * initSampleSize; 
				sample_len = RandomSampleWithDirs(files, sampleSizePerFolder, inputfiles);
			} else if (runCount == 2) {
				// Do not print EmptyFolders after the first round.
				originjob.getConfiguration().setBoolean("mapred.sample.printEmptyFolder", false);
				
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
				  
				  long time_budget = (long) (((deadline - System.currentTimeMillis()) * sample2ndRoundPctg
						  				- extraCost) * sampleTimebudgetScale);
				  if (time_budget > 0) {
					  LOG.info("Next sampleSize = " + nextSize + " (this may be inaccurate) sampleTime = " + 
							  	time_budget + " ms");
	//				  AdjustDistribution(distribution); // We do not want full distribution, neither average distribution.
					  //sample_len = RandomSampleWithDistributionByTime(files, distribution, time_budget, nextSize,
						//	  											true, inputfiles);
					  
					  if (samplingPolicy == 0) // MH
						  sample_len = RandomSampleWithDistributionByTime(files, distribution, time_budget, nextSize,
						   							  											true, inputfiles);
						  else if (samplingPolicy == 1) {  // Uniform 
//						   sample_len = RandomSampleByTime(files, distribution, time_budget, inputfiles);
//							  sample_len = RandomSample(files, (end_index-start_index+1)*sampleSizePerFolder, inputfiles);	//fy
//							  sample_number = originjob.getConfiguration().getInt("mapred.sample.number", 0);
							  sample_len = RandomSample(files, filterarr.length*sampleSizePerFolder, inputfiles);	//fy
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
				  
				  
				  long time_budget = (long) (((deadline - System.currentTimeMillis()) - extraCost)*sampleTimebudgetScale);
				  if (time_budget > 0 ){
					  LOG.info("Next sampleSize = " + nextSize + " (this may be inaccurate) sampleTime = " + 
							  	time_budget + " ms");
					  //sample_len = RandomSampleWithDistributionByTime(files, distribution, time_budget, nextSize,
							//  											true, inputfiles);

					  if (samplingPolicy == 0) // MH
						  sample_len = RandomSampleWithDistributionByTime(files, distribution, time_budget, nextSize,
						   							  											true, inputfiles);
//						  sample_len = RandomSampleWithDistribution(files, distribution, (end_index-start_index+1)*sampleSizePerFolder, 
//								  true, inputfiles);
//					  sample_len = RandomSampleWithDistribution(files, distribution, filterarr.length*sampleSizePerFolder, 
//										  false, inputfiles);
						  else if (samplingPolicy == 1) { // Uniform
						  sample_len = RandomSampleByTime(files, distribution, time_budget, inputfiles);
						   } else if (samplingPolicy == 2) {  // Same per folder
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
			
			/*if (distribution != null)
				sample_len = RandomSampleWithDistribution(files, distribution, nextSize, true, inputfiles);	
			else
				sample_len = RandomSampleWithDirs(files, nextSize, inputfiles);*/
			
			Job newjob = new Job(originjob.getConfiguration(), "sample_" + runCount);
			LOG.info(newjob.getJar());
			LOG.info("minsize = " + FileInputFormat.getMinSplitSize(newjob));
			LOG.info("maxsize = " + FileInputFormat.getMaxSplitSize(newjob));
			FileOutputFormat.setOutputPath(newjob, 
					new Path(originjob.getConfiguration().get(("mapred.output.dir")) + "_" + runCount));
			
			/*int printC = 0;
			for (SamplePath sp : inputfiles)
			{
				printC++;
				if (printC <= 20 || (printC % 1000 == 0))
					LOG.info("$$$$$$$$$$  sample is = " + sp.file_path + "; key = " + sp.sample_key + "; len = " + sp.size);
			}*/
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
			
			// all input files are included in newjob
			/*
			FileInputFormat.setInputPaths(newjob, new Path(inputfiles.get(0)));
			for (int j=1; j<inputfiles.size(); j++)
			{
				FileInputFormat.addInputPath(newjob, new Path(inputfiles.get(j)));
			}
			*/
			newjob.getConfiguration().set(SampleInputUtil.SAMP_DIR, samp_input_str);
			LOG.info("lglglglglglg: sample len = " + sample_len + ", max slot number = " + max_slotnum);
			Long splitsize = sample_len/max_slotnum;
			newjob.getConfiguration().set("mapreduce.input.fileinputformat.split.maxsize", splitsize.toString());
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
		/* start cache job first */
//		CacheJob cachejob = new CacheJob(originjob, filereclist);
//		cachejob.Start();
		
		// Clear any pre-existing stats, for example, from Caching setup job.
		originjob.clearEVStats();
				
		/* loop until deadline */
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
					  LOG.info("Next sampleSize = " + nextSize + " (this may be inaccurate) sampleTime = " + 
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
			
			/*if (distribution != null)
				sample_len = RandomSampleWithDistribution(files, distribution, nextSize, true, inputfiles);	
			else
				sample_len = RandomSampleWithDirs(files, nextSize, inputfiles);*/
			
			Job newjob = new Job(originjob.getConfiguration(), "sample_" + runCount);
			LOG.info(newjob.getJar());
			LOG.info("minsize = " + FileInputFormat.getMinSplitSize(newjob));
			LOG.info("maxsize = " + FileInputFormat.getMaxSplitSize(newjob));
			FileOutputFormat.setOutputPath(newjob, 
					new Path(originjob.getConfiguration().get(("mapred.output.dir")) + "_" + runCount));
			
			/* set input filter */
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
			/*
			FileInputFormat.setInputPaths(newjob, new Path(inputfiles.get(0)));
			for (int j=1; j<inputfiles.size(); j++)
			{
				FileInputFormat.addInputPath(newjob, new Path(inputfiles.get(j)));
			}
			*/
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
	
	// We adjust the distribution by adding every variance with the (max_var + min_var) / 2.0. 
	// This is to average the distribution a bit since we do not trust it completely. 
	  private void AdjustDistribution(Map<String, Stats> distribution) {
		double minVar = Double.MAX_VALUE;
		double maxVar = Double.MIN_VALUE;
		for (String key : distribution.keySet()) {
			double var = distribution.get(key).var;
			if (var < minVar) {
				minVar = var;
			}
			if (var > maxVar) {
				maxVar = var;
			}
		}
		double adjustedVar = (maxVar + minVar) / 2.0;
		for (String key : distribution.keySet()) {
			distribution.get(key).var += adjustedVar;
		}
		for (String key : distribution.keySet()) {
	    	 LOG.info("(after adjustment) " +  key + "\tavg = " + distribution.get(key).avg + "  var = " + distribution.get(key).var + " count = " +
	    			  distribution.get(key).count);
	     }
	}

	/**
	   * 
	   * @param pre_variable: the previously sampled variable, x_a
	   * @param cur_variable: the randomly selected "next" variable, x_b
	   * @param distribution
	   * @return
	   */
	  private String MHGetNextVariable(String cur_variable, String nxt_variable,
			  Map<String, Stats> distribution) {	  
		  if (cur_variable.equals(nxt_variable)) {
			  return cur_variable;
		  }
		  String nxt_variable_real = null;
		  double alpha_cur = (distribution.get(cur_variable).count - 1) / 2.0;
		  double alpha_nxt = (distribution.get(nxt_variable).count - 1) / 2.0;
		  double beta_cur = (distribution.get(cur_variable).count - 1) /
				  (2.0 * distribution.get(cur_variable).var);
		  double beta_nxt = (distribution.get(nxt_variable).count - 1) /
				  (2.0 * distribution.get(nxt_variable).var);
		  // lamda = sqrt( E(sigma_b^2 / sigma_a^2) )
		  double lamda = Math.sqrt( alpha_nxt * beta_cur / (beta_nxt * (alpha_cur - 1)) );
		  if (lamda >= 1.0 ) {
			  nxt_variable_real = nxt_variable;
		  } else {
			  double r = rand.nextDouble();
			  if (r <= lamda) // move to the next variable
				  nxt_variable_real = nxt_variable;
			  else // stay in the previous variable
				  nxt_variable_real = cur_variable;
		  }
		  return nxt_variable_real;
	  }
	  
	  
	  
	  
	/**
	 * Random sample num files from a FileStatus List, before we get EVStats.
	 * @param files
	 * @param filePath[]
	 * @param num
	 * @param re_list
	 * @return the size in Bytes of all files in res_list
	 */
	private Long RandomSample(List<SamplePath> files, int num, List<SamplePath> res_list)
	{
		Map<String, Double> sampledSize = new HashMap<String, Double>();
		if (num > files.size())
			num = files.size();
		Long sample_len = new Long(0);
		for(int i=0; i<num; i++)
		{
			int idx = rand.nextInt(files.size()-1);
			res_list.add(files.get(idx));
			if (files.get(idx).size>0 && files.get(idx).size <100000)
			{
				  sample_len += files.get(idx).size;
				  String filepath = files.get(idx).file_path.toString();
				  String keystr = filepath.substring(filepath.lastIndexOf("/")+1) + "/1";
				  if (!sampledSize.containsKey(keystr))
					  sampledSize.put(keystr, 1.0);
				  else
					  sampledSize.put(keystr, sampledSize.get(keystr) + 1);
			}
			else
				  LOG.info("^^^^^^^^^^  length err: " + files.get(idx).sample_key + "; " + files.get(idx).size);
			  
		}
		originjob.sampledSize.clear();
		originjob.sampledSize = sampledSize;
		return sample_len;
	}
	
		
		
		private Long RandomSampleByTime(List<SamplePath> files, Map<String, Stats> distribution,
				long time_total, List<SamplePath> res_list)
		{
			Long sample_len = new Long(0);
			long t = 0;
			while (t < time_total){
				int idx = rand.nextInt(files.size()-1);
				SamplePath fileRec = files.get(idx);
				res_list.add(fileRec);
				if (files.get(idx).size>0 && files.get(idx).size <100000)
					  sample_len += files.get(idx).size;
				else
					  LOG.info("^^^^^^^^^^  length err: " + files.get(idx).sample_key + "; " + files.get(idx).size);
				String folder = fileRec.file_path.toString();
				
				//LOG.info("filerec.filepath3 = " + folder);
				  //folder = folder.substring(folder.lastIndexOf("/")+1);
				  //folder = folder.substring(0, folder.lastIndexOf("/"));
				  folder = folder.substring(folder.lastIndexOf("/")+1);
				  folder+="/1";
				  
				  //LOG.info("folder 3 = " + folder);
				  
				//folder = folder.substring(folder.lastIndexOf("/")+1);
				
				if(distribution.get(folder)!=null)
				{
					t += distribution.get(folder).avg; // add the time cost for this variable
				}
				else
				{
					long avgTime = Long.valueOf(originjob.evStats.getAggreStat("time_per_record")); // in millisecond
					t += avgTime;
				}
			}
			return sample_len;
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
	 * Get file list based on the proportion of different dimensions (i.e., location).
	 * @return
	 */
	private Long RandomSampleWithDistribution(List<SamplePath> files, Map<String, Stats> distribution,
		 int num, boolean useMHSampling, List<SamplePath> res_list)
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
		double total_var = 0;
		for (String key : distribution.keySet()) {
			sampledSize.put(key, 0.0);
			total_var += distribution.get(key).var;	
		}
	    for (String key : distribution.keySet()) {
			sizeProportion.put(key, num * distribution.get(key).var / total_var);
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
			SamplePath fileRec = files.get(idx);
			String folder = fileRec.file_path.toString();	
			//LOG.info("filerec.filepath = " + folder);
			  //folder = folder.substring(folder.lastIndexOf("/")+1);
			  //folder = folder.substring(0, folder.lastIndexOf("/"));
			  folder = folder.substring(folder.lastIndexOf("/")+1);
			  folder+="/1";
			  //String cur_variable = folder;
			 // String cur_variable = folder; // For sequence file format.
			  //LOG.info("folder = " + folder);
			  
			//folder = folder.substring(folder.lastIndexOf("/")+1);
			boolean isChosen = false;
			if (useMHSampling) { // MH sampling algorithm
				//String cur_variable = folder;
				String cur_variable = folder; // For sequence file format.
				if (next_variable.equals("") || next_variable.equals(cur_variable)) {
					res_list.add(fileRec);
					sampledSize.put(cur_variable, sampledSize.get(cur_variable) + 1);
					if (files.get(idx).size>0 && files.get(idx).size <100000)
						  sample_len += files.get(idx).size;
					else
						LOG.info("^^^^^^^^^^  length err: " + files.get(idx).sample_key + "; " + files.get(idx).size);
					  
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
					next_variable = MHGetNextVariable(cur_variable, next_variable, distribution);
				}
				  
			} else { // Sample based on size proportion
				for (String key : sizeProportion.keySet()) {
					if (key.equals(folder) && sizeProportion.get(key) >= 1.0) {
						sizeProportion.put(key, sizeProportion.get(key) - 1.0); // decrease one from quota
						res_list.add(fileRec);
						sampledSize.put(key, sampledSize.get(key) + 1);					  
						if (files.get(idx).size>0 && files.get(idx).size <100000)
							  sample_len += files.get(idx).size;
						else
							LOG.info("^^^^^^^^^^  length err: " + files.get(idx).sample_key + "; " + files.get(idx).size);
						  
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
					if (files.get(idx).size>0 && files.get(idx).size <100000)
						 sample_len += files.get(idx).size;
					else
						LOG.info("^^^^^^^^^^  length err: " + files.get(idx).sample_key + "; " + files.get(idx).size);
					  
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
		originjob.sampledSize.clear();
		originjob.sampledSize = sampledSize;
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
	  private Long RandomSampleWithDistributionByTime(List<SamplePath> files,
			  Map<String, Stats> distribution,  long time_total, int num_total, boolean useMHSampling,
			  List<SamplePath> res_list)
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
		  double total_var = 0;
		  for (String key : distribution.keySet()) {
			  sampledSize.put(key, 0.0);
			  total_var += Math.sqrt( distribution.get(key).var);		  
		  }
		  for (String key : distribution.keySet()) {
			  sizeProportion.put(key, num_total * Math.sqrt( distribution.get(key).var) / total_var);
			 // LOG.info("RandomSample-Proportion: " + key + " " + sizeProportion.get(key));
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
			  SamplePath fileRec = files.get(idx);
			  String folder = fileRec.file_path.toString();
			  //LOG.info("filerec.filepath 1 = " + folder);
			  //folder = folder.substring(folder.lastIndexOf("/")+1);
			  //folder = folder.substring(0, folder.lastIndexOf("/"));
			  folder = folder.substring(folder.lastIndexOf("/")+1);
			  folder+="/1";
			  //String cur_variable = folder;
			  String cur_variable = folder; // For sequence file format.
			  //LOG.info("current variable 1 = " + cur_variable);
			  boolean isChosen = false;
			  if (useMHSampling) { // MH sampling algorithm			  					
				  if (distribution.containsKey(cur_variable) && 
						  (next_variable.equals("") || next_variable.equals(cur_variable))) {
					  res_list.add(fileRec);
					  sampledSize.put(cur_variable, sampledSize.get(cur_variable) + 1);	
					  if (files.get(idx).size>0 && files.get(idx).size <100000)
						  sample_len += files.get(idx).size;
					  else
						  LOG.info("^^^^^^^^^^  length err: " + files.get(idx).sample_key + "; " + files.get(idx).size);
					  time += distribution.get(cur_variable).avg; // add the time cost for this variable
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
					  next_variable = MHGetNextVariable(cur_variable, next_variable, distribution);
				  }
			  } else { // Sample based on size proportion
				  for (String key : sizeProportion.keySet()) {
					  if (key.equals(folder) && sizeProportion.get(key) >= 1.0) {
						  sizeProportion.put(key, sizeProportion.get(key) - 1.0); // decrease one from quota
						  res_list.add(fileRec);
						  sampledSize.put(key, sampledSize.get(key) + 1);					  
						  if (files.get(idx).size>0 && files.get(idx).size <100000)
							  sample_len += files.get(idx).size;
						  else
							  LOG.info("^^^^^^^^^^  length err: " + files.get(idx).sample_key + "; " + files.get(idx).size);
						  
						  time += distribution.get(cur_variable).avg; // add the time cost for this variable
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
					  if (files.get(idx).size>0 && files.get(idx).size <100000)
						  sample_len += files.get(idx).size;
					  else
						  LOG.info("^^^^^^^^^^  length err: " + files.get(idx).sample_key + "; " + files.get(idx).size);
					  
					  time += distribution.get(folder).avg; // add the time cost for this variable
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
			originjob.sampledSize.clear();
			originjob.sampledSize = sampledSize;
		  return sample_len;
	  }
	  
	  
	/**
	 * Sample with input Dirs 
	 * @param files
	 * @param numPerFolder
	 * @param res_list
	 * @return
	 */
	private Long RandomSampleWithDirs(List<SamplePath> files, int numPerFolder, List<SamplePath> res_list) {
		Map<String, Stats> sizeProportion = new HashMap<String, Stats>();
		for(String k : filereclist.keySet())
		{
			String loc = k;
			loc = loc.substring(loc.lastIndexOf("/") + 1);
			loc += "/1";
			//LOG.info("loc = " + loc);
			Stats newStats = originjob.evStats.new Stats();
			newStats.var = 1.0;
			sizeProportion.put(loc, newStats); // average among directories.
		}
		int num = numPerFolder * sizeProportion.size(); // sum = #folder * numPerFolder.
		LOG.info("Next sampleSize = " + num);
		return RandomSampleWithDistribution(files, sizeProportion, num, false, res_list);
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
		LOG.info("$$$$$$$$$$$$   home dir = " + homeDir);
		FileStatus[] mapFiles = hdfs.listStatus(homeDir);
		
		start_index = originjob.getConfiguration().getInt("mapred.sample.startindex", 0);
		end_index = originjob.getConfiguration().getInt("mapred.sample.endindex", mapFiles.length-1);
		sample_number = originjob.getConfiguration().getInt("mapred.sample.number", 0);
		sample_dis = originjob.getConfiguration().getInt("mapred.sample.distance", 1);
		
//		LOG.info("$$$$$$$$$$ start index = " + start_index);
//		LOG.info("$$$$$$$$$$ end index = " + end_index);
		
		String filter = originjob.getConfiguration().get("mapred.sample.filterlist", "");
		filterarr = filter.split(";");
	//	for (FileStatus mapfs : mapFiles)
		for (int index = 0; index < sample_number; index++)
		{
			LOG.info("$$$$$$$$$$  index = " + index);
			FileStatus mapfs = mapFiles[index * sample_dis];
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
				SamplePath sp = new SamplePath(mappath, k, length);
				reclist.add(sp);
			}
			filereclist.put(mappath.toString(), reclist);
		}
		
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
