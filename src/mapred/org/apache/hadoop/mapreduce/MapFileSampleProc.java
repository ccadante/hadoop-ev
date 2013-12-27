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
	
	private long timeConstraint; // in ms
	private double errorConstraint;
	private double errorConstraintConfidence;
	private int initSampleRound;
	// 00: MaReV + loadblance    01: w/o loadbalance
	// 10: random + loadbalance    11: random w/o loadbalance
	private int samplingPolicy; 
	private int initSampleSize;
	private int sampleSizePerFolder;
	private int expCount;
	private float sample2ndRoundPctg;
	private float splitsCoeff;
	private float splitsTimeCoeff;
	private int testLoadBalance;
	private long testLoadBalanceSize;
	private DistributedFileSystem hdfs;
	private int max_slotnum;
	private long all_input_len = 0;
	
	
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
		timeConstraint = job.getConfiguration().getInt("mapred.deadline.second", 60) * 1000;
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
		// Get coefficient of times of splits number SplitBySize, i,e., how many times of max_slots
		splitsCoeff = job.getConfiguration().getFloat("mapred.sample.splitsCoeff",
						                                                      (float) 1.5);
		// Get coefficient of times of splits number for SplitByTime i,e., how many times of max_slots
		splitsTimeCoeff = job.getConfiguration().getFloat("mapred.sample.splitsTimeCoeff",
								                                               (float) 1.0);
		// 0 - not test (normal execution); 1 - splitByTime; 2 - splitBySize
		testLoadBalance = job.getConfiguration().getInt("mapred.sample.testLoadBalance", 0);
		testLoadBalanceSize = job.getConfiguration().getLong("mapred.sample.testLoadBalanceSize",
							  1 * 1024 * 1024); // in Byte
		  
		// Get the sampling policy (algorithm): 0: MaReV  1: uniform (proportion to folder)  2: same size per folder
		samplingPolicy = job.getConfiguration().getInt("mapred.sample.policy", 0);
		
		expCount = job.getConfiguration().getInt("mapred.sample.experimentCount", 1);
		// Get number of Map slots in the cluster.
		int datanode_num = hdfs.getDataNodeStats().length;
		int max_mapnum = job.getConfiguration().getInt("mapred.tasktracker.map.tasks.maximum", 2);
		max_slotnum = datanode_num * max_mapnum;
		LOG.info("folder_num = " + filereclist.size() + "  datanode_num = " + datanode_num + "  max_mapnum = " + max_mapnum);
		
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
			long deadline = System.currentTimeMillis() + timeConstraint; // in millisecond
			long timer = System.currentTimeMillis();
			long N = files.size(); // Total input records number.	
			
			// Clear any pre-existing stats, for example, from Caching setup job.
			originjob.clearEVStats();
			long extraCost = 0L;
			LOG.info("*** Deadline - " + (timeConstraint / 1000) + " ***");
			LOG.info("*** Policy - " + samplingPolicy + " ***");
			LOG.info("*** Experiment - " + expC + "/" + expCount + " ***");
			String suffix = "-" + (timeConstraint / 1000) + "s-" + samplingPolicy + "p";
			if(testLoadBalance > 0) {
				LOG.info("*** testLoadBalanceSize: " + (testLoadBalanceSize / 1024 / 1024) + " ***");
				LOG.info("*** testLoadBalance: " + testLoadBalance + " ***");				
				if (testLoadBalance == 1) { // by time 
					LOG.info("*** splitsCoeff: " + splitsTimeCoeff + " ***");
					suffix = "-" + testLoadBalance + "-" + (testLoadBalanceSize / 1024 / 1024) + "M-"
							+ splitsTimeCoeff + "C";
				}
				else if (testLoadBalance == 2) { // by size
					LOG.info("*** splitsCoeff: " + splitsCoeff + " ***");
					suffix = "-" + testLoadBalance + "-" + (testLoadBalanceSize / 1024 / 1024) + "M-"
							+ splitsCoeff + "C";
				}
			}
			
			int[] timeBudgetForRandom = new int[10];
			
			long computationTime = 0;
			while(System.currentTimeMillis() < deadline)
			//while(computationTime < timeConstraint && System.currentTimeMillis() < deadline)
			{		
				runCount += 1; 				
				LOG.debug("*** Deadline - " + (timeConstraint / 1000) + " ***");
				LOG.debug("*** Policy - " + samplingPolicy + " ***");
				LOG.debug("*** Experiment - " + expC + "/" + expCount + " ***");
				LOG.debug("*** Round - " + runCount + " ***");	
				
				long totalTimeCost = System.currentTimeMillis() - timer;
				timer = System.currentTimeMillis();				
				LOG.debug("To deadline: " + (deadline - System.currentTimeMillis()) + " ms");
				  
				// myStats format: Time average, value variance, Time count!
				Map<String, Stats> myStats = originjob.processEVStats(false);
				long avgTime = Long.valueOf(originjob.evStats.getAggreStat("time_per_record")); // in millisecond
				originjob.getConfiguration().set("mapred.sample.avgTimeCost", String.valueOf(avgTime));
				int totalSize = Integer.valueOf(originjob.evStats.getAggreStat("total_size")); 
				long firstMapperTime = Long.valueOf(originjob.evStats.getAggreStat("first_mapper_time")); // in millisecond
				long lastMapperTime = Long.valueOf(originjob.evStats.getAggreStat("last_mapper_time")); // in millisecond
				long avgReducerTime = Long.valueOf(originjob.evStats.getAggreStat("avg_reducer_time")); // in millisecond
					 
				long mapTime = (lastMapperTime - firstMapperTime);
				long reduceTime = avgReducerTime; // only one reducer
				long extraCostTemp = totalTimeCost - (mapTime + reduceTime);				
				// Note some rounds may be skipped, so totalTimeCost is small.
				if (extraCostTemp > 500) { 
					extraCost = extraCostTemp;
					computationTime += mapTime + reduceTime;
				}
				LOG.debug("avgCost = " + avgTime + "ms   recordSize = " + totalSize +
						  "   extraCost = " + extraCost + "ms   totalCost = " + totalTimeCost + "ms");
				
				/*if (computationTime >= timeConstraint)
					break;*/
				
				//LOG.debug("To timeConstraint: " + (timeConstraint - computationTime) + " ms");
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
				}
				// 0-run  1-skip  2-break
				int run_skip_break = 0;
				switch (samplingPolicy) {
					case 00: 
					case 01: {						
						if (runCount == 1) {
							sample_results = SamplingAlg.evenSampleWithDirs(files, myStats,
									sampleSizePerFolder, inputfiles, filereclist, originjob);
							sample_len = sample_results[0];
							sample_time = sample_results[1];
						} else if (runCount == 2) {
							time_budget = (long) (((deadline - System.currentTimeMillis()) * sample2ndRoundPctg
					  				- extraCost));
							if (time_budget < 5000) {
								run_skip_break = 1;
								break;
							}
							LOG.debug("Next sampleTime = " + time_budget + " ms (x " + max_slotnum + ")");					  
							sample_results = SamplingAlg.evenSampleWithDirsByTime(files, myStats,
									  SamplingAlg.K_0_1, time_budget * max_slotnum, false, avgTime, inputfiles, filereclist, originjob);
							sample_len = sample_results[0];
							sample_time = sample_results[1];
						} else if (runCount == 3){
							time_budget = (long) (((deadline - System.currentTimeMillis()) - extraCost));
							if (time_budget < 2000) {
								run_skip_break = 2;
							}
							time_budget = Math.max(time_budget, 2000);
							LOG.debug("Next sampleTime = " + time_budget + " ms (x " + max_slotnum + ")");			  
							sample_results = SamplingAlg.sampleWithDistributionByTime(
									  files, myStats, time_budget * max_slotnum, false, inputfiles, filereclist, originjob);
						  	sample_len = sample_results[0];
						  	sample_time = sample_results[1];
						}
						break;
					}
					case 10 : 
					case 11 : {
						//int maxRound = 10;
						if (runCount == 1) {
							int roundTotal = 2 + rand.nextInt(8);
							int timeTotal = (int) (deadline - System.currentTimeMillis() - 2 * 10000);
							timeTotal = Math.max(timeTotal, 2 * 2000);
							for (int i = 0; i<roundTotal - 1; i++) {
								int timeCur = rand.nextInt(timeTotal / (roundTotal - i));
								timeBudgetForRandom[i] = timeCur;
								timeTotal -= timeCur;
							}
							timeBudgetForRandom[roundTotal-1] = timeTotal;
							String timeBudget = "";
							for (int timeCur : timeBudgetForRandom) {
								timeBudget += timeCur + " ";
							}
							LOG.debug("timeBudgetForRandom: " + timeBudget);
							
							int maxSize =(int) ((timeBudgetForRandom[runCount-1] * max_slotnum - filereclist.size() * 1500) / 400);
							sample_results = SamplingAlg.randomSampleByNum(files, myStats,
									maxSize, inputfiles, filereclist, originjob);
							sample_len = sample_results[0];
							sample_time = sample_results[1];
						} else if (runCount >= 2) {
							time_budget = timeBudgetForRandom[runCount - 1];
							time_budget = Math.min(time_budget, (deadline - System.currentTimeMillis() - extraCost));
							if (time_budget < 2000) {
								run_skip_break = 2;
							}
							LOG.debug("Next sampleTime = " + time_budget + " ms (x " + max_slotnum + ")");					  
							sample_results = SamplingAlg.randomSampleByTime(files, myStats,
									time_budget * max_slotnum, avgTime, inputfiles, filereclist, originjob);
							sample_len = sample_results[0];
							sample_time = sample_results[1];
						}
						
						/*if (runCount == 1) {
							int maxSize =(int) (((deadline - System.currentTimeMillis()) * max_slotnum - filereclist.size() * 1500) / 450);
							maxSize = rand.nextInt(maxSize / (1 + rand.nextInt(maxRound)));
							sample_results = SamplingAlg.randomSampleByNum(files, myStats,
									maxSize, inputfiles, filereclist, originjob);
							sample_len = sample_results[0];
							sample_time = sample_results[1];
						} else if (runCount == 2){
							time_budget = (long) ((deadline - System.currentTimeMillis()) - extraCost);
							if (time_budget < 1000) {
								run_skip_break = 1;
								break;
							}
							time_budget = rand.nextInt((int)time_budget / (1 + rand.nextInt(maxRound)));
							LOG.debug("Next sampleTime = " + time_budget + " ms (x " + max_slotnum + ")");					  
							sample_results = SamplingAlg.randomSampleByTime(files, myStats,
									time_budget * max_slotnum, avgTime, inputfiles, filereclist, originjob);
							sample_len = sample_results[0];
							sample_time = sample_results[1];
						} else {
							time_budget = (long) ((deadline - System.currentTimeMillis())- extraCost);
							if (time_budget < 1000) {
								run_skip_break = 2;
								break;
							}
							time_budget = rand.nextInt((int)(time_budget) / (1 + rand.nextInt(maxRound)));
							LOG.debug("Next sampleTime = " + time_budget + " ms (x " + max_slotnum + ")");					  
							sample_results = SamplingAlg.randomSampleByTime(files, myStats,
									time_budget * max_slotnum, avgTime, inputfiles, filereclist, originjob);
							sample_len = sample_results[0];
							sample_time = sample_results[1];		
						}*/
						break;
					}
					default : {
						LOG.warn("Unknown sampling policy - " + samplingPolicy + ". Break!");
						break;
					}					
				}			
				if (run_skip_break == 1) {
					LOG.debug("Skip round - " + runCount + ".");
					continue;
				} else if (run_skip_break == 2) {
					LOG.debug("Break round - " + runCount + ".");
					break;
				} 
				
				if (testLoadBalance > 0 && runCount == 2) {		
					LOG.info("testLoadBalance clear : " + testLoadBalanceSize);
					inputfiles.clear();
					sample_results = SamplingAlg.randomSampleBySize(
							  files, myStats, testLoadBalanceSize, inputfiles, filereclist, originjob);
					sample_len = sample_results[0];
					sample_time = sample_results[1];
				}
				
				Job newjob = new Job(originjob.getConfiguration(),
						"sample" + suffix + "_" + expC + "_" + runCount);
				//LOG.info("Jar: " + newjob.getJar());
				FileOutputFormat.setOutputPath(newjob, 
						new Path(originjob.getConfiguration().get(("mapred.output.dir"))
								+ suffix + "_" + expC + "_" + runCount));
				
				/* set input file path */;
				if (inputfiles.size() == 0) 
					continue;
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
				
				switch (samplingPolicy) {
					case 00:
					case 10: {
						if (runCount == 1) { // split by size
							Long splitsize = (long) (sample_len / max_slotnum / splitsCoeff);
							newjob.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", splitsize);
						} else { 					
							newjob.getConfiguration().setBoolean("mapred.input.fileinputformat.splitByTime", true);
							newjob.getConfiguration().setLong("mapred.input.fileinputformat.splitByTime.maxTime",
									(long)(sample_time / max_slotnum / splitsTimeCoeff));
							// Desired number of splits based on time.
							newjob.getConfiguration().setInt("mapreduce.input.fileinputformat.split.number", max_slotnum);
							// Add EVStats to Job configuration for splitting based on Time rather than size!
							addMapTimeCostToJob(newjob, myStats);
						}
						break;
					}
					case 01:
					case 11: {
						Long splitsize = (long) (sample_len / max_slotnum / splitsCoeff);
						newjob.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", splitsize);
						break;
					}
					default : {
						
					}
				}
				
				// For testLoadBalance
				if (testLoadBalance > 0 && runCount == 2) {					
					if (testLoadBalance == 1) { // split by time
						newjob.getConfiguration().setBoolean("mapred.input.fileinputformat.splitByTime", true);
						newjob.getConfiguration().setLong("mapred.input.fileinputformat.splitByTime.maxTime",
								(long)(sample_time / max_slotnum / splitsTimeCoeff * 1.10));
						newjob.getConfiguration().setInt("mapreduce.input.fileinputformat.split.number", max_slotnum);
						addMapTimeCostToJob(newjob, myStats);
					} else if (testLoadBalance == 2) { // split by size
						newjob.getConfiguration().setBoolean("mapred.input.fileinputformat.splitByTime", false);
						Long splitsize = (long) (sample_len / max_slotnum / splitsCoeff);
						newjob.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", splitsize);
					}
				}
				
				// Run job!
				newjob.waitForCompletion(true);
				  
				double[] results = originjob.processReduceResults(inputfiles.size(), N, OpType.SUM, false);
				LOG.debug("Result estimation: sum(avg(Loc)) = " + results[0] + "+-" + results[1] + 
						  " (95% confidence).\n");
				
				// testLoadBalance only has two rounds!
				if (testLoadBalance > 0 && runCount >= 2) {
					break;
				}
			}
			long timeDiff = System.currentTimeMillis() - deadline;		
			LOG.info("Final deadline diff: " + timeDiff + "ms");		
			
			Map<String, Stats> myStats = originjob.processEVStats(true);
			long avgTime = Long.valueOf(originjob.evStats.getAggreStat("time_per_record")); // in millisecond
			int totalSize = Integer.valueOf(originjob.evStats.getAggreStat("total_size")); 
			long firstMapperTime = Long.valueOf(originjob.evStats.getAggreStat("first_mapper_time")); // in millisecond
			long lastMapperTime = Long.valueOf(originjob.evStats.getAggreStat("last_mapper_time")); // in millisecond
			long avgReducerTime = Long.valueOf(originjob.evStats.getAggreStat("avg_reducer_time")); // in millisecond
			
			long totalTimeCost = System.currentTimeMillis() - timer;			
			long mapTime = (lastMapperTime - firstMapperTime);
			long reduceTime = avgReducerTime; // only one reducer
			long extraCostTemp = totalTimeCost - (mapTime + reduceTime);				
			// Note some rounds may be skipped, so totalTimeCost is small.
			if (extraCostTemp > 500) { 
				extraCost = extraCostTemp;
				computationTime += mapTime + reduceTime;
			}			
			LOG.debug("avgCost = " + avgTime + "ms   recordSize = " + totalSize +
					  "   extraCost = " + extraCost + "ms   totalCost = " + totalTimeCost + "ms");
			/*long timeDiff = computationTime - timeConstraint;
			 LOG.info("Final timeConstraint diff: " + timeDiff + "ms");*/
			
			double[] results = originjob.processReduceResults(0, N, OpType.SUM, true);
			LOG.info("FINAL RESULT ESTIMATION: sum(avg(Loc)) = " + results[0] + "+-" + results[1] + 
					  " (95% confidence).");
			if (testLoadBalance > 0) {
				LOG.info("FINAL COMPUTATION TIME: " + (lastMapperTime - firstMapperTime) + "ms");
			}
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
	public static boolean isValidDataSize(long length) {
		if (length < 1000 || length > 300000) {
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
