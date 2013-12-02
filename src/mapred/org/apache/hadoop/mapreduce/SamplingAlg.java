package org.apache.hadoop.mapreduce;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.EVStatistics.Stats;
import org.apache.hadoop.mapreduce.lib.input.SamplePath;

public class SamplingAlg {
	public static final Log LOG = LogFactory.getLog(SamplingAlg.class);
	
	static Random rand = new Random();
	
	/**
	 * Some image may be corrupted with invalid file size.
	 */
	public static boolean isValidFileSize(long size) {
		if (size > 1000 && size < 150000)
			return true;
		else
			return false;
	}
	
	private static long getTimeFromStats(String key, Map<String, Stats> distribution) {
		if (distribution.containsKey(key)) {
			return (long)(distribution.get(key).getAvg());
		} else {
			return 0;
		}
	}
	
	/**
	   * 
	   * @param pre_variable: the previously sampled variable, x_a
	   * @param cur_variable: the randomly selected "next" variable, x_b
	   * @param distribution
	   * @return
	   */
	  public static String MHGetNextVariable(String cur_variable, String nxt_variable,
			  Map<String, Stats> distribution)
	  {	  
		  if (cur_variable.equals(nxt_variable)) {
			  return cur_variable;
		  }
		  String nxt_variable_real = null;
		  /*double alpha_cur = (distribution.get(cur_variable).count - 1) / 2.0;
		  double alpha_nxt = (distribution.get(nxt_variable).count - 1) / 2.0;
		  double beta_cur = (distribution.get(cur_variable).count - 1) /
				  (2.0 * distribution.get(cur_variable).var);
		  double beta_nxt = (distribution.get(nxt_variable).count - 1) /
				  (2.0 * distribution.get(nxt_variable).var);
		  // lamda = sqrt( E(sigma_b^2 / sigma_a^2) )
		  double lamda = Math.sqrt( alpha_nxt * beta_cur / (beta_nxt * (alpha_cur - 1)) );*/
		  double lamda = distribution.get(nxt_variable).getStd() / distribution.get(cur_variable).getStd();
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
	 * @param distribution 
	 * @param filePath[]
	 * @param num
	 * @param re_list
	 * @return the size in Bytes of all files in res_list
	 */
	public static Long[] RandomSample(List<SamplePath> files, Map<String, Stats> distribution,
			int num, List<SamplePath> res_list, Job originjob)
	{
		Map<String, Double> sampledSize = new HashMap<String, Double>();
		if (num > files.size())
			num = files.size();
		Long sample_len = new Long(0);
		Long sample_time = new Long(0);
		for(int i=0; i<num; i++)
		{
			int idx = rand.nextInt(files.size()-1);
			res_list.add(files.get(idx));
			if (isValidFileSize(files.get(idx).size))
			{
				  sample_len += files.get(idx).size;
				  String filepath = files.get(idx).file_path.toString();
				  String keystr = filepath.substring(filepath.lastIndexOf("/")+1) + "/1";
				  sample_time += getTimeFromStats(keystr, distribution);
				  if (!sampledSize.containsKey(keystr))
					  sampledSize.put(keystr, 1.0);
				  else
					  sampledSize.put(keystr, sampledSize.get(keystr) + 1);
			}
			else {
				LOG.info("^^^^^^^^^^  length err: " + files.get(idx).sample_key + "  " + files.get(idx).size);
			}
			  
		}
		originjob.sampledSize.clear();
		originjob.sampledSize = sampledSize;
		return (new Long[]{sample_len, sample_time});
	}
	
	public static Long[] RandomSampleByTime(List<SamplePath> files, Map<String, Stats> distribution,
			long time_total, List<SamplePath> res_list, Job originjob)
	{
		Long sample_len = new Long(0);
		Long sample_time = new Long(0);
		long t = 0;
		while (t < time_total){
			int idx = rand.nextInt(files.size()-1);
			SamplePath fileRec = files.get(idx);
			res_list.add(fileRec);
			String folder = fileRec.file_path.toString();
			folder = folder.substring(folder.lastIndexOf("/")+1);
			folder+="/1";
			
			if (isValidFileSize(files.get(idx).size)) {
				  sample_len += files.get(idx).size;
				  sample_time += getTimeFromStats(folder, distribution);
			}
			else
				  LOG.info("^^^^^^^^^^  length err: " + files.get(idx).sample_key + "; " + files.get(idx).size);
			
			if(distribution.get(folder)!=null)
			{
				t += distribution.get(folder).getAvg(); // add the time cost for this variable
			}
			else
			{
				long avgTime = Long.valueOf(originjob.evStats.getAggreStat("time_per_record")); // in millisecond
				t += avgTime;
			}
		}
		return (new Long[]{sample_len, sample_time});
	}
	
	
	/**
	 * Get file list based on the proportion of different dimensions (i.e., location).
	 * @return
	 */
	public static Long[] RandomSampleWithDistribution(List<SamplePath> files,
			Map<String, Stats> distribution, int num, boolean useMHSampling, List<SamplePath> res_list,
			Job originjob)
	{
		if (distribution == null || distribution.size() == 0)
		{
			return RandomSample(files, distribution, num, res_list, originjob);
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
		Long sample_time = new Long(0);
		String next_variable = ""; // The next should sampled variable by MH, x_i, e.g., camera-loc.
		while(count > 0.99)
		{
			int idx = rand.nextInt(files.size());
			SamplePath fileRec = files.get(idx);
			String folder = fileRec.file_path.toString();	
			folder = folder.substring(folder.lastIndexOf("/")+1);
			folder+="/1";
			boolean isChosen = false;
			if (useMHSampling) { // MH sampling algorithm
				String cur_variable = folder; // For sequence file format.
				if (next_variable.equals("") || next_variable.equals(cur_variable)) {
					res_list.add(fileRec);
					sampledSize.put(cur_variable, sampledSize.get(cur_variable) + 1);
					if (isValidFileSize(files.get(idx).size)) {
						  sample_len += files.get(idx).size;
						  sample_time += getTimeFromStats(cur_variable, distribution);
					}
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
						if (isValidFileSize(files.get(idx).size)) {
							  sample_len += files.get(idx).size;
							  sample_time += getTimeFromStats(key, distribution);
						}
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
					if (isValidFileSize(files.get(idx).size)) {
						 sample_len += files.get(idx).size;
						 sample_time += getTimeFromStats(folder, distribution);
					}
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
		return (new Long[]{sample_len, sample_time});
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
	public static Long[] RandomSampleWithDistributionByTime(List<SamplePath> files,
			  Map<String, Stats> distribution,  long time_total, int num_total, boolean useMHSampling,
			  List<SamplePath> res_list, Job originjob)
	  {
		  if (distribution == null || distribution.size() == 0) {
			  return RandomSample(files, distribution, 0, res_list, originjob);
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
		  Long sample_time = new Long(0);
		  String next_variable = ""; // The next should sampled variable by MH, x_i, e.g., camera-loc.
		  while(time < time_total && count > 0.99)
		  {
			  int idx = rand.nextInt(files.size());
			  SamplePath fileRec = files.get(idx);
			  String folder = fileRec.file_path.toString();
			  folder = folder.substring(folder.lastIndexOf("/")+1);
			  folder+="/1";
			  String cur_variable = folder; // For sequence file format.
			  boolean isChosen = false;
			  if (useMHSampling) { // MH sampling algorithm			  					
				  if (distribution.containsKey(cur_variable) && 
						  (next_variable.equals("") || next_variable.equals(cur_variable))) {
					  res_list.add(fileRec);
					  sampledSize.put(cur_variable, sampledSize.get(cur_variable) + 1);	
					  if (isValidFileSize(files.get(idx).size)) {
						  sample_len += files.get(idx).size;
						  sample_time += getTimeFromStats(cur_variable, distribution);
					  }
					  else
						  LOG.info("^^^^^^^^^^  length err: " + files.get(idx).sample_key + "; " + files.get(idx).size);
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
					  next_variable = MHGetNextVariable(cur_variable, next_variable, distribution);
				  }
			  } else { // Sample based on size proportion
				  for (String key : sizeProportion.keySet()) {
					  if (key.equals(folder) && sizeProportion.get(key) >= 1.0) {
						  sizeProportion.put(key, sizeProportion.get(key) - 1.0); // decrease one from quota
						  res_list.add(fileRec);
						  sampledSize.put(key, sampledSize.get(key) + 1);					  
						  if (isValidFileSize(files.get(idx).size)) {
							  sample_len += files.get(idx).size;
							  sample_time += getTimeFromStats(key, distribution);
						  }
						  else
							  LOG.info("^^^^^^^^^^  length err: " + files.get(idx).sample_key + "; " + files.get(idx).size);
						  
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
					  if (isValidFileSize(files.get(idx).size)) {
						  sample_len += files.get(idx).size;
						  sample_time += getTimeFromStats(folder, distribution);
					  }
					  else
						  LOG.info("^^^^^^^^^^  length err: " + files.get(idx).sample_key + "; " + files.get(idx).size);
					  
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
			originjob.sampledSize.clear();
			originjob.sampledSize = sampledSize;
		  return (new Long[]{sample_len, sample_time});
	  }
	  
	  
	/**
	 * Sample with input Dirs 
	 * @param files
	 * @param distribution 
	 * @param numPerFolder
	 * @param res_list
	 * @return
	 */
	public static Long[] RandomSampleWithDirs(List<SamplePath> files, Map<String, Stats> stats,
			int numPerFolder, List<SamplePath> res_list,
			Hashtable<String, List<SamplePath>> filereclist, Job originjob) {
		Map<String, Stats> sizeProportion = new HashMap<String, Stats>();
		for(String k : filereclist.keySet())
		{
			String folder = k;
			folder = folder.substring(folder.lastIndexOf("/") + 1);
			folder += "/1";
			//LOG.info("loc = " + loc);
			Stats newStats = originjob.evStats.new Stats();
			newStats.setVar(1.0);
			newStats.setAvg(0.0);
			// Update with time cost from EVStats
			if (stats.containsKey(folder)) {
				newStats.setAvg(stats.get(folder).getAvg());
			}
			sizeProportion.put(folder, newStats); // average among directories.
		}
		int num = numPerFolder * sizeProportion.size(); // sum = #folder * numPerFolder.
		LOG.info("Next sampleNumber = " + num);
		return RandomSampleWithDistribution(files, sizeProportion, num, false, res_list, originjob);
	}
}
