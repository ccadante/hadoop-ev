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
import org.mortbay.log.Log;

/**
 * 
 * SequenceFileInputFilter process
 * the Key is a formated string "camera_id/time_stamp"
 * @author fan
 *
 */
public class SequenceFileSampleProc {

	private Job originjob;
	private HashMap<String, List<SequenceFileRecord>> keyreclist 
					= new HashMap<String, List<SequenceFileRecord>>();
	private long totalfilenum = 0;
	
	private int timeConstraint;
	private int initSampleRound;
	private int initSampleSize;
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
		// Get trial/initial sample rounds number.
		initSampleRound = job.getConfiguration().getInt("mapred.sample.initround", 1);
	    // Get trial/initial rounds sample unit size.
		initSampleSize = job.getConfiguration().getInt("mapred.sample.initsize", 60);
		// Get number of Map slots in the cluster.
		hdfs = (DistributedFileSystem)(FileSystem.get(job.getConfiguration()));
		
		int datanode_num = hdfs.getDataNodeStats().length;
		int max_mapnum = job.getConfiguration().getInt("mapred.tasktracker.map.tasks.maximum", 2);
		max_slotnum = datanode_num*max_mapnum;
		if (max_slotnum <= 0) {
			Log.info("Can not read number of slots!  datanode_num=" + datanode_num +
					  " max_mapnum=" + max_mapnum);
			return false;
		}
		return true;
	}
	
	public boolean start() throws IOException, InterruptedException, ClassNotFoundException
	{
		int runCount = 0;
		long deadline = System.currentTimeMillis() + timeConstraint * 1000; // in millisecond
		long timer = System.currentTimeMillis();
		
		/* start cache job first */
//		CacheJob cachejob = new CacheJob(originjob, keyreclist);
//		cachejob.Start();
		
		/* loop until deadline */
		while(System.currentTimeMillis() < deadline)
		{		
			runCount++;
			long totalTimeCost = System.currentTimeMillis() - timer;
			timer = System.currentTimeMillis();
			long extraCost = 0;
			Log.info("To deadline: " + (deadline - System.currentTimeMillis()) + " ms");
			  
			Map<String, Stats> distribution = null;
			int nextSize = 1;
			if (runCount <= initSampleRound) {
				nextSize = runCount * initSampleSize;
			} 
			else 
			{
				distribution = originjob.processEVStats();
				long avgTime = Long.valueOf(originjob.evStats.getAggreStat("time_per_record")); // in millisecond
				int totalSize = Integer.valueOf(originjob.evStats.getAggreStat("total_size")); 			  
				// NOTE: when computing extraCost and nextSize, we need to consider the number of parallel
				// Map slots.
				if (avgTime > 0) {
					extraCost = totalTimeCost - avgTime * totalSize / max_slotnum; // in millisecond
					nextSize = (int) ((deadline - System.currentTimeMillis() - extraCost)
							  / avgTime * max_slotnum);
				}
				Log.info("avgCost = " + avgTime + "ms ; recordSize = " + totalSize +
						  " ; extraCost = " + extraCost + "ms");
			}
			Log.info("Next sampleSize = " + nextSize);		
			if (nextSize <= 0) {
				Log.info("Quit!");
				break;
			}
			// get the files total size in a sample and determine the proper split size
			List<SequenceFileRecord> inputfiles = new ArrayList<SequenceFileRecord>();
			Long sample_len = 0L;
			List<SequenceFileRecord> files = GetWholeFileRecordList();
			long N = files.size(); // Total input records size.
			if (distribution != null)
				sample_len = RandomSampleWithDistribution(files, distribution, nextSize, true, inputfiles);	
			else
				sample_len = RandomSampleWithDirs(files, nextSize, inputfiles);
			Long splitsize = all_input_len/max_slotnum;
			Log.info("max slot number = " + max_slotnum + "; split size = " + splitsize);
			
			Job newjob = new Job(originjob.getConfiguration(), "sample_" + runCount);
			Log.info(newjob.getJar());
			newjob.getConfiguration().set("mapred.min.split.size", splitsize.toString());
			Log.info("minsize = " + FileInputFormat.getMinSplitSize(newjob));
			Log.info("maxsize = " + FileInputFormat.getMaxSplitSize(newjob));
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
			  
			double[] results = originjob.processReduceResults(inputfiles.size(), N, OpType.AVG);
			Log.info("RESULT ESTIMATION: sum(avg(Loc)) = " + results[0] + "+-" + results[1] + 
					  " (95% confidence).\n");
		}
		long timeDiff = System.currentTimeMillis() - deadline;
		if (timeDiff >= 0)
			Log.info("After deadline: " + Math.abs(timeDiff) + "ms");
		else
			Log.info("Before deadline: " + Math.abs(timeDiff) + "ms");
		  
		return true;
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
		  // lamda = E(sigma_b^2 / sigma_a^2)
		  double lamda = alpha_nxt * beta_cur / (beta_nxt * (alpha_cur - 1));
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
		double total_var = 0;
		for (String key : distribution.keySet()) {
			sampledSize.put(key, 0.0);
			total_var += distribution.get(key).var;	
		}
	    for (String key : distribution.keySet()) {
			sizeProportion.put(key, num * distribution.get(key).var / total_var);
			Log.info("RandomSample-Proportion: " + key + " " + sizeProportion.get(key));
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
					next_variable = MHGetNextVariable(cur_variable, next_variable, distribution);
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
			Log.info("RandomSample-final: " + key + " " + sampledSize.get(key));
		}
		return sample_len;
	}
	  
	  
	/**
	 * Sample with input Dirs 
	 * @param files
	 * @param num
	 * @param res_list
	 * @return
	 */
	private Long RandomSampleWithDirs(List<SequenceFileRecord> files, int num, List<SequenceFileRecord> res_list) {
		Map<String, Stats> sizeProportion = new HashMap<String, Stats>();
		for(int i=0; i<files.size(); i++)
		{
			String loc =files.get(i).getReduceKey();
			Stats newStats = originjob.evStats.new Stats();
			newStats.var = 1.0;
			sizeProportion.put(loc, newStats); // average among directories.
		}
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
				Log.info("Values are compressed.");
	        }
            if (reader.isBlockCompressed()) 
            {
            	Log.info("Records are block-compressed.");
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
		Log.info("File number = " + totalfilenum);
	}  
	
	
	
	  
	
	// Get the folder name from a full path name, which is the deepest directory name.
	private String GetFolderFromFullPath(String path) {
		String folder = null;
		try {
			folder = path;
			folder = folder.substring(0, folder.lastIndexOf("/"));
//			folder = folder.substring(folder.lastIndexOf("/")+1);
		} catch (Exception ex) {
			  Log.info("GetFolderFromFullPath:" + ex.getMessage());
		}
		return folder;
	}
	  
	
}
