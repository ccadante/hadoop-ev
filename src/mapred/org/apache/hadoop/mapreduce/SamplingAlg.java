package org.apache.hadoop.mapreduce;

import java.util.ArrayList;
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
	
	final static Random rand = new Random();
	
	final static int K_0_1 = 200; // The value to let 1.96*Std/mean < 0.1 for normal distribution
	final public static int TIME_FILE_LOAD_FIRST = 1500;
	final public static int TIME_FILE_LOAD = 20;
	
	/**
	 * Some image may be corrupted with invalid file size.
	 */
	public static boolean isValidFileSize(long size) {
		return MapFileSampleProc.isValidDataSize(size);
	}
	
	private static long getTimeFromStats(String key, Map<String, Stats> distribution, Job originjob) {
		if (distribution.containsKey(key)) {
			return (long)(distribution.get(key).getAvg());
		} else {
			long avgTimeCost = originjob.getConfiguration().getLong("mapred.sample.avgTimeCost", 300);
			return avgTimeCost;
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
	public static Long[] randomSampleByNum(List<SamplePath> files, Map<String, Stats> distribution,
			int num, List<SamplePath> res_list,
			Hashtable<String, List<SamplePath>> filereclist, Job originjob)
	{
		if (num > files.size())
			num = files.size();
		Long sample_len = new Long(0);
		Long sample_time = new Long(0);
		// Record the number of actual samples.
		Map<String, Double> sampledSize = new HashMap<String, Double>();
		for (String key : filereclist.keySet()) {
			sampledSize.put(key, 0.0);
		}
		// Guarantee at least 1 sample per folder
		for (String key : filereclist.keySet()){
			int idx = rand.nextInt(filereclist.get(key).size());
			SamplePath fileRec = filereclist.get(key).get(idx);
			String folder = fileRec.file_path.toString();	
			folder = folder.substring(folder.lastIndexOf("/")+1);
			folder+="/1";		
			res_list.add(fileRec);
			sampledSize.put(folder, sampledSize.get(folder) + 1);
			sample_len += fileRec.size;
			sample_time += getTimeFromStats(folder, distribution, originjob) + TIME_FILE_LOAD;
			sample_time += TIME_FILE_LOAD_FIRST;
		}
		for(int i=filereclist.size(); i<num; i++)
		{
			int idx = rand.nextInt(files.size()-1);
			res_list.add(files.get(idx));
			if (isValidFileSize(files.get(idx).size))
			{
				  sample_len += files.get(idx).size;
				  String filepath = files.get(idx).file_path.toString();
				  String keystr = filepath.substring(filepath.lastIndexOf("/")+1) + "/1";
				  sample_time += getTimeFromStats(keystr, distribution, originjob);
				  if (!sampledSize.containsKey(keystr))
					  sampledSize.put(keystr, 1.0);
				  else
					  sampledSize.put(keystr, sampledSize.get(keystr) + 1);
			}
			else {
				//LOG.debug("^^^^^^^^^^  length err: " + files.get(idx).sample_key + "  " + files.get(idx).size);
			}
			  
		}
		for (String key : sampledSize.keySet()) {
			LOG.debug("RandomSample-final: " + key + " " + sampledSize.get(key));
		}
		originjob.sampledSize.clear();
		originjob.sampledSize = sampledSize;
		return (new Long[]{sample_len, sample_time});
	}
	
	public static Long[] randomSampleByTime(List<SamplePath> files, Map<String, Stats> distribution,
			long time_total, long avg_time_cost, List<SamplePath> res_list,
			Hashtable<String, List<SamplePath>> filereclist, Job originjob)
	{
		Long sample_len = new Long(0);
		Long sample_time = new Long(0);
		// Record the number of actual samples.
		Map<String, Double> sampledSize = new HashMap<String, Double>();
		for (String key : filereclist.keySet()) {
			sampledSize.put(key, 0.0);
		}
		// Guarantee at least 1 sample per folder
		for (String key : filereclist.keySet()){
			int idx = rand.nextInt(filereclist.get(key).size());
			SamplePath fileRec = filereclist.get(key).get(idx);
			String folder = fileRec.file_path.toString();	
			folder = folder.substring(folder.lastIndexOf("/")+1);
			folder+="/1";		
			res_list.add(fileRec);
			sampledSize.put(folder, sampledSize.get(folder) + 1);
			sample_len += fileRec.size;
			sample_time += getTimeFromStats(folder, distribution, originjob) + TIME_FILE_LOAD;
			sample_time += TIME_FILE_LOAD_FIRST;
		}
		while (sample_time < time_total){
			int idx = rand.nextInt(files.size());			
			SamplePath fileRec = files.get(idx);
			String folder = fileRec.file_path.toString();
			folder = folder.substring(folder.lastIndexOf("/")+1);
			folder+="/1";
			
			if (!isValidFileSize(files.get(idx).size)) {
				//LOG.debug("^^^^^^^^^^  length err: " + files.get(idx).sample_key + "; " + files.get(idx).size);
				continue;				  
			}
			sample_len += files.get(idx).size;
			sample_time += getTimeFromStats(folder, distribution, originjob) + TIME_FILE_LOAD;
			res_list.add(fileRec);		
			
			if(distribution.get(folder)!=null)
				sampledSize.put(folder, sampledSize.get(folder) + 1);
			else
				sampledSize.put(folder, 1.0);
		}
		for (String key : sampledSize.keySet()) {
			LOG.debug("RandomSample-final: " + key + " " + sampledSize.get(key));
		}
		originjob.sampledSize.clear();
		originjob.sampledSize = sampledSize;
		return (new Long[]{sample_len, sample_time});
	}
	
	public static Long[] randomSampleBySize(List<SamplePath> files, Map<String, Stats> distribution,
			long length_total, List<SamplePath> res_list,
			Hashtable<String, List<SamplePath>> filereclist, Job originjob)
	{
		Long sample_len = new Long(0);
		Long sample_time = new Long(0);
		// Record the number of actual samples.
		Map<String, Double> sampledSize = new HashMap<String, Double>();
		for (String key : filereclist.keySet()) {
			sampledSize.put(key, 0.0);
		}
		// Guarantee at least 1 sample per folder
		for (String key : filereclist.keySet()){
			int idx = rand.nextInt(filereclist.get(key).size());
			SamplePath fileRec = filereclist.get(key).get(idx);
			String folder = fileRec.file_path.toString();	
			folder = folder.substring(folder.lastIndexOf("/")+1);
			folder+="/1";		
			res_list.add(fileRec);
			sampledSize.put(folder, sampledSize.get(folder) + 1);
			sample_len += fileRec.size;
			sample_time += getTimeFromStats(folder, distribution, originjob) + TIME_FILE_LOAD;
			sample_time += TIME_FILE_LOAD_FIRST;
		}
		while (sample_len < length_total){
			int idx = rand.nextInt(files.size());			
			SamplePath fileRec = files.get(idx);
			String folder = fileRec.file_path.toString();
			folder = folder.substring(folder.lastIndexOf("/")+1);
			folder+="/1";
			
			if (!isValidFileSize(files.get(idx).size)) {
				//LOG.debug("^^^^^^^^^^  length err: " + files.get(idx).sample_key + "; " + files.get(idx).size);
				continue;				  
			}
			sample_len += files.get(idx).size;
			sample_time += getTimeFromStats(folder, distribution, originjob);
			res_list.add(fileRec);		
			
			if(distribution.get(folder)!=null)
			{
				sampledSize.put(folder, sampledSize.get(folder) + 1);
			}
			else
			{
				sampledSize.put(folder, 1.0);
			}
		}
		for (String key : sampledSize.keySet()) {
			LOG.debug("RandomSample-final: " + key + " " + sampledSize.get(key));
		}
		originjob.sampledSize.clear();
		originjob.sampledSize = sampledSize;
		return (new Long[]{sample_len, sample_time});
	}
	
	
	/**
	 * Get file list based on the proportion of different dimensions (i.e., location).
	 * @return
	 */
	public static Long[] sampleWithDistribution(List<SamplePath> files,
			Map<String, Stats> distribution, int num, boolean useMHSampling, List<SamplePath> res_list,
			Hashtable<String, List<SamplePath>> filereclist, Job originjob)
	{
		if (distribution == null || distribution.size() == 0)
		{
			return randomSampleByNum(files, distribution, num, res_list, filereclist, originjob);
		}
		if (num > files.size())
			num = files.size();
		// Record the number of actual samples.
		Map<String, Double> sampledSize = new HashMap<String, Double>();
		// Get sample sizes from proportion distribution.
		Map<String, Double> sizeProportion = new HashMap<String, Double>();
		// Record the keys in an array
		ArrayList<String> keyList = new ArrayList<String>();
		double total_std = 0;
		for (String key : distribution.keySet()) {
			keyList.add(key);
			sampledSize.put(key, 0.0);
			total_std += distribution.get(key).getStd();	
		}
	    for (String key : distribution.keySet()) {
			sizeProportion.put(key, num * distribution.get(key).getStd() / total_std);
			//LOG.debug("RandomSample-Proportion: " + key + " " + sizeProportion.get(key));
		}
		
		Long sample_len = new Long(0);
		Long sample_time = new Long(0);
		sample_time += TIME_FILE_LOAD_FIRST * distribution.size();
		for (String key: sizeProportion.keySet()) {
			long countFolder = Math.max(Math.round(sizeProportion.get(key)), 1);
			while(countFolder > 0) {
				int idx = rand.nextInt(filereclist.get(key).size());
				SamplePath fileRec = filereclist.get(key).get(idx);
				String folder = fileRec.file_path.toString();	
				folder = folder.substring(folder.lastIndexOf("/")+1);
				folder+="/1";				
				if (!isValidFileSize(fileRec.size)) {
					//LOG.debug("^^^^^^^^^^  length err: " + files.get(idx).sample_key + "; " + files.get(idx).size);
					continue;						  
				}
				countFolder--;
				res_list.add(fileRec);
				sampledSize.put(folder, sampledSize.get(folder) + 1);
				sample_len += fileRec.size;
				sample_time += getTimeFromStats(folder, distribution, originjob) + TIME_FILE_LOAD;
			}
		}		
		for (String key : sampledSize.keySet()) {
			LOG.debug("RandomSample-final: " + key + " " + sampledSize.get(key));
		}
		originjob.sampledSize.clear();
		originjob.sampledSize = sampledSize;
		return (new Long[]{sample_len, sample_time});
	}
	
	private static boolean isAllDone(Map<String, Double> sizeProportion) {
		for (String key : sizeProportion.keySet()) {
			if (sizeProportion.get(key) >= 1.0) {
				return false;
			}
		}
		return true;
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
	public static Long[] sampleWithDistributionByTime(List<SamplePath> files,
			  Map<String, Stats> distribution,  long time_total, int max_slotnum,
			  boolean useMHSampling, List<SamplePath> res_list,
			  Hashtable<String, List<SamplePath>> filereclist, Job originjob)
	  {
		  if (distribution == null || distribution.size() == 0) {
			  return randomSampleByNum(files, distribution, 0, res_list, filereclist, originjob);
		  }
		  // Record the number of actual samples.
		  Map<String, Double> sampledSize = new HashMap<String, Double>();
		  // Get sample sizes from proportion distribution.
		  Map<String, Double> sizeProportion = new HashMap<String, Double>();
		  // Record the keys in an array
		  ArrayList<String> keyList = new ArrayList<String>();		  
		  double toal_std_time = 0;
		  for (String key : distribution.keySet()) {
			  sampledSize.put(key, 0.0);
			   // \sum{std*sqrt{timeCost}}
			  double std_time = distribution.get(key).getStd() * Math.sqrt(distribution.get(key).getAvg());	
			  toal_std_time += std_time;
			  keyList.add(key);
		  }
		  int nonZeroFolderC = 0;
		  for (String key : distribution.keySet()) {			  
			  // std / sqrt(timeCost)
			  double std_by_time = distribution.get(key).getStd() / Math.sqrt(distribution.get(key).getAvg());
			  double count = Math.round(std_by_time / toal_std_time * time_total);
			  if (count >= 1) {
				  nonZeroFolderC++;
			  }
			  sizeProportion.put(key, count);
			  //LOG.debug("RandomSample-Proportion: " + key + " " + sizeProportion.get(key));
		  }
		  long ori_total = time_total;
		  time_total -= TIME_FILE_LOAD_FIRST * nonZeroFolderC;
		  double kappa = 1.0;
		  time_total = Math.max(time_total, 1000 * max_slotnum);
		  kappa = time_total / (double) ori_total;	  
		  for (String key : distribution.keySet()) {			  
			  sizeProportion.put(key, sizeProportion.get(key) * kappa);
			  //LOG.debug("RandomSample-Proportion: " + key + " " + sizeProportion.get(key));
		  }		  
		  Long sample_len = new Long(0);
		  Long sample_time = new Long(0);
		  for (String key: sizeProportion.keySet()) {
			  long countFolder = Math.round(sizeProportion.get(key));
			  boolean isFirst = true;
				while(countFolder > 0) { 					
					int idx = rand.nextInt(filereclist.get(key).size());
					SamplePath fileRec = filereclist.get(key).get(idx);
					String folder = fileRec.file_path.toString();	
					folder = folder.substring(folder.lastIndexOf("/")+1);
					folder+="/1";				
					if (!isValidFileSize(fileRec.size)) {
						//LOG.debug("^^^^^^^^^^  length err: " + files.get(idx).sample_key + "; " + files.get(idx).size);
						continue;						  
					}
					countFolder--;
					res_list.add(fileRec);
					sampledSize.put(folder, sampledSize.get(folder) + 1);
					sample_len += fileRec.size;
					sample_time += getTimeFromStats(folder, distribution, originjob) + TIME_FILE_LOAD;
					if (isFirst) {
						sample_time += TIME_FILE_LOAD_FIRST;
						isFirst = false;
					}
				}
			}
		  for (String key : sampledSize.keySet()) {
			  LOG.debug("RandomSample-final: " + key + " " + sampledSize.get(key));
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
	public static Long[] evenSampleWithDirs(List<SamplePath> files, Map<String, Stats> stats,
			int numPerFolder, List<SamplePath> res_list,
			Hashtable<String, List<SamplePath>> filereclist, Job originjob) {
		Map<String, Stats> sizeProportion = new HashMap<String, Stats>();
		for(String k : filereclist.keySet())
		{
			String folder = k;
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
		LOG.debug("Next sampleNumber = " + num);
		return sampleWithDistribution(files, sizeProportion, num, false,
				res_list, filereclist, originjob);
	}
	
	
	public static Long[] evenSampleWithDirsByTime(List<SamplePath> files, Map<String, Stats> stats,
			int numPerFolder, long total_time, boolean use_all_time, long avg_time_cost,
			List<SamplePath> res_list, Hashtable<String, List<SamplePath>> filereclist, Job originjob) {		
		Map<String, Stats> sizeProportion = new HashMap<String, Stats>();
		long time_cost = 0;
		for(String k : filereclist.keySet())
		{
			String folder = k;
			//LOG.debug("loc = " + loc);
			Stats newStats = originjob.evStats.new Stats();
			newStats.setVar(1.0);
			newStats.setAvg(0.0);
			// Update with time cost from EVStats
			if (stats.containsKey(folder)) {
				double cost = stats.get(folder).getAvg();
				newStats.setAvg(cost);
				time_cost += numPerFolder * (cost + TIME_FILE_LOAD);
			} else 
				time_cost += numPerFolder * (avg_time_cost + TIME_FILE_LOAD);
			sizeProportion.put(folder, newStats); // average among directories.
		}
		total_time = total_time - TIME_FILE_LOAD_FIRST * sizeProportion.size();
		double kappa = 1.0;	
		if (use_all_time) { // scale to the total_time
			kappa = total_time / (double) time_cost;
		} else {
			if (time_cost > total_time) 
				kappa = total_time / (double) time_cost;
		}
		int num = (int) (numPerFolder * sizeProportion.size() * kappa); // sum = #folder * numPerFolder
		LOG.debug("time_cost = " + time_cost + "  total_time = " + total_time + "  kappa = " + kappa);
		LOG.debug("Next sampleNumber = " + num);
		return sampleWithDistribution(files, sizeProportion, num, false,
				res_list, filereclist, originjob);
	}
}
