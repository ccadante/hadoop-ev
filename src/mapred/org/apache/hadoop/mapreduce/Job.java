/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.EVStatistics.Stats;
import org.apache.hadoop.mapreduce.EVStatistics.StatsType;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.DirUtil;
import org.apache.hadoop.mapred.EVStatsServer;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.hsqldb.lib.StringUtil;
//import org.mortbay.log.Log;

/**
 * The job submitter's view of the Job. It allows the user to configure the
 * job, submit it, control its execution, and query the state. The set methods
 * only work until the job is submitted, afterwards they will throw an 
 * IllegalStateException.
 */
public class Job extends JobContext {  
	public static final Log LOG = LogFactory.getLog(Job.class);
	
  public static enum JobState {DEFINE, RUNNING};
  private JobState state = JobState.DEFINE;
  private JobClient jobClient;
  private RunningJob info;
  
  //EVStatsServer port (TODO: start a bunch of ports to handle concurrent connections?)
  private int portEVStatsServer; 
  private static EVStatsServer evStatsServer;

  //A integer indicating the number of DataProcesses in EVStatsServer
  public int dpInProc = 0;
  
  public Job() throws IOException {
    this(new Configuration());
  }

  public Job(Configuration conf) throws IOException {
	  super(conf, null);
	  if (evStatsServer == null) {
	    // Start EVStatsServer
	    Random rand = new Random();
	    this.portEVStatsServer = 10593 + rand.nextInt(1000);
	    getConfiguration().setInt("mapred.evstats.serverport", portEVStatsServer);
	    evStatsServer = new EVStatsServer(this.portEVStatsServer, this);
	    evStatsServer.start();	  
	  }    
  }

  public Job(Configuration conf, String jobName) throws IOException {
    this(conf);
    setJobName(jobName);
  }

  JobClient getJobClient() {
    return jobClient;
  }
  
  private void ensureState(JobState state) throws IllegalStateException {
    if (state != this.state) {
      throw new IllegalStateException("Job in state "+ this.state + 
                                      " instead of " + state);
    }

    if (state == JobState.RUNNING && jobClient == null) {
      throw new IllegalStateException("Job in state " + JobState.RUNNING + 
                                      " however jobClient is not initialized!");
    }
  }

  /**
   * Set the number of reduce tasks for the job.
   * @param tasks the number of reduce tasks
   * @throws IllegalStateException if the job is submitted
   */
  public void setNumReduceTasks(int tasks) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setNumReduceTasks(tasks);
  }

  /**
   * Set the current working directory for the default file system.
   * 
   * @param dir the new current working directory.
   * @throws IllegalStateException if the job is submitted
   */
  public void setWorkingDirectory(Path dir) throws IOException {
    ensureState(JobState.DEFINE);
    conf.setWorkingDirectory(dir);
  }

  /**
   * Set the {@link InputFormat} for the job.
   * @param cls the <code>InputFormat</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setInputFormatClass(Class<? extends InputFormat> cls
                                  ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(INPUT_FORMAT_CLASS_ATTR, cls, InputFormat.class);
  }

  /**
   * Set the {@link OutputFormat} for the job.
   * @param cls the <code>OutputFormat</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setOutputFormatClass(Class<? extends OutputFormat> cls
                                   ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(OUTPUT_FORMAT_CLASS_ATTR, cls, OutputFormat.class);
  }

  /**
   * Set the {@link Mapper} for the job.
   * @param cls the <code>Mapper</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setMapperClass(Class<? extends Mapper> cls
                             ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(MAP_CLASS_ATTR, cls, Mapper.class);
  }

  /**
   * Set the Jar by finding where a given class came from.
   * @param cls the example class
   */
  public void setJarByClass(Class<?> cls) {
    conf.setJarByClass(cls);
  }
  
  /**
   * Get the pathname of the job's jar.
   * @return the pathname
   */
  public String getJar() {
    return conf.getJar();
  }

  /**
   * Set the combiner class for the job.
   * @param cls the combiner to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setCombinerClass(Class<? extends Reducer> cls
                               ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(COMBINE_CLASS_ATTR, cls, Reducer.class);
  }

  /**
   * Set the {@link Reducer} for the job.
   * @param cls the <code>Reducer</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setReducerClass(Class<? extends Reducer> cls
                              ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(REDUCE_CLASS_ATTR, cls, Reducer.class);
  }

  /**
   * Set the {@link Partitioner} for the job.
   * @param cls the <code>Partitioner</code> to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setPartitionerClass(Class<? extends Partitioner> cls
                                  ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(PARTITIONER_CLASS_ATTR, cls, Partitioner.class);
  }

  /**
   * Set the key class for the map output data. This allows the user to
   * specify the map output key class to be different than the final output
   * value class.
   * 
   * @param theClass the map output key class.
   * @throws IllegalStateException if the job is submitted
   */
  public void setMapOutputKeyClass(Class<?> theClass
                                   ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setMapOutputKeyClass(theClass);
  }

  /**
   * Set the value class for the map output data. This allows the user to
   * specify the map output value class to be different than the final output
   * value class.
   * 
   * @param theClass the map output value class.
   * @throws IllegalStateException if the job is submitted
   */
  public void setMapOutputValueClass(Class<?> theClass
                                     ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setMapOutputValueClass(theClass);
  }

  /**
   * Set the key class for the job output data.
   * 
   * @param theClass the key class for the job output data.
   * @throws IllegalStateException if the job is submitted
   */
  public void setOutputKeyClass(Class<?> theClass
                                ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setOutputKeyClass(theClass);
  }

  /**
   * Set the value class for job outputs.
   * 
   * @param theClass the value class for job outputs.
   * @throws IllegalStateException if the job is submitted
   */
  public void setOutputValueClass(Class<?> theClass
                                  ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setOutputValueClass(theClass);
  }

  /**
   * Define the comparator that controls how the keys are sorted before they
   * are passed to the {@link Reducer}.
   * @param cls the raw comparator
   * @throws IllegalStateException if the job is submitted
   */
  public void setSortComparatorClass(Class<? extends RawComparator> cls
                                     ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setOutputKeyComparatorClass(cls);
  }

  /**
   * Define the comparator that controls which keys are grouped together
   * for a single call to 
   * {@link Reducer#reduce(Object, Iterable, 
   *                       org.apache.hadoop.mapreduce.Reducer.Context)}
   * @param cls the raw comparator to use
   * @throws IllegalStateException if the job is submitted
   */
  public void setGroupingComparatorClass(Class<? extends RawComparator> cls
                                         ) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setOutputValueGroupingComparator(cls);
  }

  /**
   * Set the user-specified job name.
   * 
   * @param name the job's new name.
   * @throws IllegalStateException if the job is submitted
   */
  public void setJobName(String name) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setJobName(name);
  }
  
  /**
   * Turn speculative execution on or off for this job. 
   * 
   * @param speculativeExecution <code>true</code> if speculative execution 
   *                             should be turned on, else <code>false</code>.
   */
  public void setSpeculativeExecution(boolean speculativeExecution) {
    ensureState(JobState.DEFINE);
    conf.setSpeculativeExecution(speculativeExecution);
  }

  /**
   * Turn speculative execution on or off for this job for map tasks. 
   * 
   * @param speculativeExecution <code>true</code> if speculative execution 
   *                             should be turned on for map tasks,
   *                             else <code>false</code>.
   */
  public void setMapSpeculativeExecution(boolean speculativeExecution) {
    ensureState(JobState.DEFINE);
    conf.setMapSpeculativeExecution(speculativeExecution);
  }

  /**
   * Turn speculative execution on or off for this job for reduce tasks. 
   * 
   * @param speculativeExecution <code>true</code> if speculative execution 
   *                             should be turned on for reduce tasks,
   *                             else <code>false</code>.
   */
  public void setReduceSpeculativeExecution(boolean speculativeExecution) {
    ensureState(JobState.DEFINE);
    conf.setReduceSpeculativeExecution(speculativeExecution);
  }

  /**
   * Get the URL where some job progress information will be displayed.
   * 
   * @return the URL where some job progress information will be displayed.
   */
  public String getTrackingURL() {
    ensureState(JobState.RUNNING);
    return info.getTrackingURL();
  }

  /**
   * Get the <i>progress</i> of the job's setup, as a float between 0.0 
   * and 1.0.  When the job setup is completed, the function returns 1.0.
   * 
   * @return the progress of the job's setup.
   * @throws IOException
   */
  public float setupProgress() throws IOException {
    ensureState(JobState.RUNNING);
    return info.setupProgress();
  }

  /**
   * Get the <i>progress</i> of the job's map-tasks, as a float between 0.0 
   * and 1.0.  When all map tasks have completed, the function returns 1.0.
   * 
   * @return the progress of the job's map-tasks.
   * @throws IOException
   */
  public float mapProgress() throws IOException {
    ensureState(JobState.RUNNING);
    return info.mapProgress();
  }

  /**
   * Get the <i>progress</i> of the job's reduce-tasks, as a float between 0.0 
   * and 1.0.  When all reduce tasks have completed, the function returns 1.0.
   * 
   * @return the progress of the job's reduce-tasks.
   * @throws IOException
   */
  public float reduceProgress() throws IOException {
    ensureState(JobState.RUNNING);
    return info.reduceProgress();
  }

  /**
   * Check if the job is finished or not. 
   * This is a non-blocking call.
   * 
   * @return <code>true</code> if the job is complete, else <code>false</code>.
   * @throws IOException
   */
  public boolean isComplete() throws IOException {
    ensureState(JobState.RUNNING);
    return info.isComplete();
  }

  /**
   * Check if the job completed successfully. 
   * 
   * @return <code>true</code> if the job succeeded, else <code>false</code>.
   * @throws IOException
   */
  public boolean isSuccessful() throws IOException {
    ensureState(JobState.RUNNING);
    return info.isSuccessful();
  }

  /**
   * Kill the running job.  Blocks until all job tasks have been
   * killed as well.  If the job is no longer running, it simply returns.
   * 
   * @throws IOException
   */
  public void killJob() throws IOException {
    ensureState(JobState.RUNNING);
    info.killJob();
  }
    
  /**
   * Get events indicating completion (success/failure) of component tasks.
   *  
   * @param startFrom index to start fetching events from
   * @return an array of {@link TaskCompletionEvent}s
   * @throws IOException
   */
  public TaskCompletionEvent[] getTaskCompletionEvents(int startFrom
                                                       ) throws IOException {
    ensureState(JobState.RUNNING);
    return info.getTaskCompletionEvents(startFrom);
  }
  
  /**
   * Kill indicated task attempt.
   * 
   * @param taskId the id of the task to be terminated.
   * @throws IOException
   */
  public void killTask(TaskAttemptID taskId) throws IOException {
    ensureState(JobState.RUNNING);
    info.killTask(org.apache.hadoop.mapred.TaskAttemptID.downgrade(taskId), 
                  false);
  }

  /**
   * Fail indicated task attempt.
   * 
   * @param taskId the id of the task to be terminated.
   * @throws IOException
   */
  public void failTask(TaskAttemptID taskId) throws IOException {
    ensureState(JobState.RUNNING);
    info.killTask(org.apache.hadoop.mapred.TaskAttemptID.downgrade(taskId), 
                  true);
  }

  /**
   * Gets the counters for this job.
   * 
   * @return the counters for this job.
   * @throws IOException
   */
  public Counters getCounters() throws IOException {
    ensureState(JobState.RUNNING);
    return new Counters(info.getCounters());
  }

  private void ensureNotSet(String attr, String msg) throws IOException {
    if (conf.get(attr) != null) {
      throw new IOException(attr + " is incompatible with " + msg + " mode.");
    }    
  }
  
  /**
   * Sets the flag that will allow the JobTracker to cancel the HDFS delegation
   * tokens upon job completion. Defaults to true.
   */
  public void setCancelDelegationTokenUponJobCompletion(boolean value) {
    ensureState(JobState.DEFINE);
    conf.setBoolean(JOB_CANCEL_DELEGATION_TOKEN, value);
  }

  /**
   * Default to the new APIs unless they are explicitly set or the old mapper or
   * reduce attributes are used.
   * @throws IOException if the configuration is inconsistant
   */
  private void setUseNewAPI() throws IOException {
    int numReduces = conf.getNumReduceTasks();
    String oldMapperClass = "mapred.mapper.class";
    String oldReduceClass = "mapred.reducer.class";
    conf.setBooleanIfUnset("mapred.mapper.new-api",
                           conf.get(oldMapperClass) == null);
    if (conf.getUseNewMapper()) {
      String mode = "new map API";
      ensureNotSet("mapred.input.format.class", mode);
      ensureNotSet(oldMapperClass, mode);
      if (numReduces != 0) {
        ensureNotSet("mapred.partitioner.class", mode);
       } else {
        ensureNotSet("mapred.output.format.class", mode);
      }      
    } else {
      String mode = "map compatability";
      ensureNotSet(JobContext.INPUT_FORMAT_CLASS_ATTR, mode);
      ensureNotSet(JobContext.MAP_CLASS_ATTR, mode);
      if (numReduces != 0) {
        ensureNotSet(JobContext.PARTITIONER_CLASS_ATTR, mode);
       } else {
        ensureNotSet(JobContext.OUTPUT_FORMAT_CLASS_ATTR, mode);
      }
    }
    if (numReduces != 0) {
      conf.setBooleanIfUnset("mapred.reducer.new-api",
                             conf.get(oldReduceClass) == null);
      if (conf.getUseNewReducer()) {
        String mode = "new reduce API";
        ensureNotSet("mapred.output.format.class", mode);
        ensureNotSet(oldReduceClass, mode);   
      } else {
        String mode = "reduce compatability";
        ensureNotSet(JobContext.OUTPUT_FORMAT_CLASS_ATTR, mode);
        ensureNotSet(JobContext.REDUCE_CLASS_ATTR, mode);   
      }
    }   
  }

  /**
   * Submit the job to the cluster and return immediately.
   * @throws IOException
   */
  public void submit() throws IOException, InterruptedException, 
                              ClassNotFoundException {
    ensureState(JobState.DEFINE);
    setUseNewAPI();
    
    // Connect to the JobTracker and submit the job
    connect();
    info = jobClient.submitJobInternal(conf);
    super.setJobID(info.getID());
    state = JobState.RUNNING;
   }
  
  /**
   * Open a connection to the JobTracker
   * @throws IOException
   * @throws InterruptedException 
   */
  private void connect() throws IOException, InterruptedException {
    ugi.doAs(new PrivilegedExceptionAction<Object>() {
      public Object run() throws IOException {
        jobClient = new JobClient((JobConf) getConfiguration());    
        return null;
      }
    });
  }
  
  /**
   * Submit the job to the cluster and wait for it to finish.
   * @param verbose print the progress to the user
   * @return true if the job succeeded
   * @throws IOException thrown if the communication with the 
   *         <code>JobTracker</code> is lost
   */
  public boolean waitForCompletion(boolean verbose
                                   ) throws IOException, InterruptedException,
                                            ClassNotFoundException {
    if (state == JobState.DEFINE) {
      submit();
    }
    if (verbose) {
      jobClient.monitorAndPrintJob(conf, info);
    } else {
      info.waitForCompletion();
    }
    return isSuccessful();
  }
  
  /**
   * Get the sample strategy and submit a series of jobs
   * @throws ClassNotFoundException 
   * @throws InterruptedException 
   * @throws IOException 
   * @throws CloneNotSupportedException 
   */
  public boolean waitForSampleCompletion() throws IOException, InterruptedException, ClassNotFoundException, CloneNotSupportedException
  {
	  String dirs = this.getConfiguration().get("mapred.input.dir", "");
	  String [] list = StringUtils.split(dirs);
	  
	  // Get time constraint.
	  int timeConstraint = this.getConfiguration().getInt("mapred.deadline.second", 150);
	  // Get trial/initial sample rounds number.
	  int initSampleRound = this.getConfiguration().getInt("mapred.sample.initround", 1);
	  // Get trial/initial rounds sample unit size.
	  int initSampleSize = this.getConfiguration().getInt("mapred.sample.initsize", 60);
	  // Get number of Map slots in the cluster.
	  DistributedFileSystem hdfs = (DistributedFileSystem)(FileSystem.get(this.getConfiguration()));
	  int datanode_num = hdfs.getDataNodeStats().length;
	  int max_mapnum = this.getConfiguration().getInt("mapred.tasktracker.map.tasks.maximum", 2);
	  int max_slotnum = datanode_num*max_mapnum;
	  if (max_slotnum <= 0) {
		  LOG.fatal("Can not read number of slots!  datanode_num=" + datanode_num +
				  " max_mapnum=" + max_mapnum);
		  return false;
	  }
	  
	  long deadline = System.currentTimeMillis() + timeConstraint * 1000; // in millisecond
	  LOG.info("Deadline: " + deadline + " (" + timeConstraint + "s)");
	  
	  Configuration conf = this.getConfiguration();
	  InputFormat<?, ?> input = ReflectionUtils.newInstance(this.getInputFormatClass(), conf);

	  List<FileStatus> files = ((FileInputFormat)input).getListStatus(this);
	   
	  long N = files.size(); // Total input records size.
	  int runCount = 0;
	  long timer = System.currentTimeMillis();
	  
	  LOG.info("File number = " + N);
	  
	  CacheJob cachejob = new CacheJob(this, files);
	  cachejob.Start();
	  
	  // loop until deadline.
	  while(System.currentTimeMillis() < deadline)
	  {		
		  runCount++;
		  long totalTimeCost = System.currentTimeMillis() - timer;
		  timer = System.currentTimeMillis();
		  long extraCost = 0;
		  LOG.info("To deadline: " + (deadline - System.currentTimeMillis()) + " ms");
		  
		  Map<String, Stats> distribution = null;
		  int nextSize = 1;
		  if (runCount <= initSampleRound) {
			  nextSize = runCount * initSampleSize;
		  } else {
			  distribution = processEVStats();
			  long avgTime = Long.valueOf(evStats.getAggreStat("time_per_record")); // in millisecond
			  int totalSize = Integer.valueOf(evStats.getAggreStat("total_size")); 			  
			  // NOTE: when computing extraCost and nextSize, we need to consider the number of parallel
			  // Map slots.
			  if (avgTime > 0) {
				  extraCost = totalTimeCost - avgTime * totalSize / max_slotnum; // in millisecond
				  nextSize = (int) ((deadline - System.currentTimeMillis() - extraCost)
						  / avgTime * max_slotnum);
			  }
			  LOG.info("avgCost = " + avgTime + "ms ; recordSize = " + totalSize +
					  " ; extraCost = " + extraCost + "ms");
		  }
		  LOG.info("Next sampleSize = " + nextSize);		
		  if (nextSize <= 0) {
			  LOG.info("Quit!");
			  break;
		  }
		  // get the files total size in a sample and determine the proper split size
		  List<String> inputfiles = new ArrayList<String>();
		  Long sample_len = 0L;
		  if (distribution != null)
			  sample_len = RandomSampleWithDistribution(files, distribution, nextSize, true, inputfiles);	
		  else
			  sample_len = RandomSampleWithDirs(files, nextSize, inputfiles);
		  Long splitsize = sample_len/max_slotnum;
		  LOG.info("max slot number = " + max_slotnum + "; split size = " + splitsize);
		  
		  Job newjob = new Job(this.getConfiguration(), "sample_" + runCount);
		  LOG.info(newjob.getJar() + " " + list.length);
		  newjob.getConfiguration().set("mapreduce.input.fileinputformat.split.maxsize", splitsize.toString());
		  FileOutputFormat.setOutputPath(newjob, new Path(this.getConfiguration().get(("mapred.output.dir")) + "_" + runCount));
		  FileInputFormat.setInputPaths(newjob, new Path(inputfiles.get(0)));
		  for (int j=1; j<inputfiles.size(); j++)
		  {
			  FileInputFormat.addInputPath(newjob, new Path(inputfiles.get(j)));
		  }
			
		  newjob.waitForCompletion(true);
		  
		  double[] results = processReduceResults(inputfiles.size(), N, OpType.AVG);
		  LOG.info("RESULT ESTIMATION: sum(avg(Loc)) = " + results[0] + "+-" + results[1] + 
				  " (95% confidence).\n");
	  }
	  long timeDiff = System.currentTimeMillis() - deadline;
	  if (timeDiff >= 0)
		  LOG.info("After deadline: " + Math.abs(timeDiff) + "ms");
	  else
		  LOG.info("Before deadline: " + Math.abs(timeDiff) + "ms");
	  
	  while (dpInProc > 0) {}
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
  private Long RandomSample(List<FileStatus> files, int num, List<String> res_list)
  {
	  if (num > files.size())
		  num = files.size();
	  Long sample_len = new Long(0);
	  for(int i=0; i<num; i++)
	  {
		  int idx = rand.nextInt(files.size()-1);
		  res_list.add(HDFStoLocalConvert(files.get(idx).getPath().toString()));
		  sample_len += files.get(idx).getLen();
	  }
	  return sample_len;
  }
  
  /**
   * Get file list based on the proportion of different dimensions (i.e., location).
   * @return
   */
  private Long RandomSampleWithDistribution(List<FileStatus> files,
		  Map<String, Stats> distribution,
		  int num, boolean useMHSampling, List<String> res_list)
  {
	  if (distribution == null || distribution.size() == 0) {
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
		  String filename = HDFStoLocalConvert(files.get(idx).getPath().toString());
		  String folder = DirUtil.GetLast2ndSeg(filename);
		  boolean isChosen = false;
		  if (useMHSampling) { // MH sampling algorithm
			  String cur_variable = folder;
			  if (next_variable.equals("") || next_variable.equals(cur_variable)) {
				  res_list.add(filename);
				  sampledSize.put(cur_variable, sampledSize.get(cur_variable) + 1);
				  sample_len += files.get(idx).getLen();
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
					  res_list.add(filename);
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
				  res_list.add(filename);
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
  
  // Get the folder name from a full path name, which is the deepest directory name.
  private String GetFolderFromFullPath(String path) {
	  String folder = null;
	  try {
		  folder = path;
		  folder = folder.substring(0, folder.lastIndexOf("/"));
		  folder = folder.substring(folder.lastIndexOf("/")+1);
	  } catch (Exception ex) {
		  LOG.error("GetFolderFromFullPath:" + ex.getMessage());
	  }
	  return folder;
  }
  
  /**
   * Sample with input Dirs 
   * @param files
   * @param num
   * @param res_list
   * @return
   */
  private Long RandomSampleWithDirs(List<FileStatus> files, int num, List<String> res_list) {
	  Map<String, Stats> sizeProportion = new HashMap<String, Stats>();
	  for(int i=0; i<files.size(); i++)
	  {
		  String loc = DirUtil.GetLast2ndSeg(files.get(i).getPath().toString());
		  Stats newStats = evStats.new Stats();
		  newStats.var = 1.0;
		  sizeProportion.put(loc, newStats); // average among directories.
	  }
	  return RandomSampleWithDistribution(files, sizeProportion, num, false, res_list);
  }
  /**
   * Convert HDFS dir to local dir
   */
  private String HDFStoLocalConvert(String HDFSStr)
  {
	  int iter = 5;
	  int pos = -1;
	  for (int i=0; i<iter; i++)
	  {
		  pos = HDFSStr.indexOf("/", pos+1);
	  }
	  //LOG.info(HDFSStr.substring(pos+1));
	  return HDFSStr.substring(pos+1);
  }
 
  // The size of evStatsSet should equal number of Map tasks.
  Set<EVStatistics> evStatsSet = new HashSet<EVStatistics>();
  EVStatistics evStats = new EVStatistics();
  ArrayList<ArrayList<Double>> reduceResults = new ArrayList<ArrayList<Double>>(); 
  Random rand = new Random(); // It is better to share a global Random class.
  
  /**
   * Add one piece of EVStats into the global set.
   * @param evStat
   */
  public void addEVStats(EVStatistics evStat){
	  if (evStat == null || evStat.getSize() == 0) {
		  LOG.warn("Got a null/empty stat.");
		  return;
	  }
	  synchronized (Job.this){
		  evStatsSet.add(evStat);
		  //LOG.warn("addEVStats set size = " + evStatsSet.size());
	  }
  }
  
  /**
   * Process current set of EVStats to get the time-cost distribution across different domain. 
   * Currently, we aggregate data for each folder (i.e, camera location).
   * @return Map<String, Double>, e.g., <"16m_1", 0.90>, <"16m_1", 0.099>
   */
  private Map<String, Stats> processEVStats(){
	  
	  long actualSize = 0;
	  Map<String, Stats> tmp_stat = new HashMap<String, Stats>();
	  Map<String, Stats> final_stat = new HashMap<String, Stats>();
	  
	  // Compute average.
      for (EVStatistics evstat : evStatsSet) {
    	  actualSize += evstat.getSize();
    	  for (StatsType type : evstat.timeProfile.keySet()) {
    		  String loc = GetFolderFromFullPath(type.value);
    		  if (!tmp_stat.containsKey(loc)) {
    			  tmp_stat.put(loc, evStats.new Stats());
    		  }
    		  tmp_stat.get(loc).addValue(evstat.timeProfile.get(type) / 1000); // us to ms
    	  }
      }
      for (String key : tmp_stat.keySet()) {
    	  tmp_stat.get(key).computeAvg();
      }        
      // Compute variance.
      for (EVStatistics evstat : evStatsSet) {
    	  for (StatsType type : evstat.timeProfile.keySet()) {
    		  String loc = GetFolderFromFullPath(type.value);
    		  tmp_stat.get(loc).addDiff(evstat.timeProfile.get(type) / 1000); // us to ms
    	  }
      }
      for (String key : tmp_stat.keySet()) {
    	  tmp_stat.get(key).computeVar();
      }
      
      // NOTE: we want to remove outlier data!
      // Compute average.
      long avgTime = 0;
	  long totalSize = 0;
      for (EVStatistics evstat : evStatsSet) {
    	  for (StatsType type : evstat.timeProfile.keySet()) {
    		  String loc = GetFolderFromFullPath(type.value);
    		  if (!final_stat.containsKey(loc)) {
    			  final_stat.put(loc, evStats.new Stats());
    		  }
    		  // Diff(val - avg) < 2 * std 
    		  long val = evstat.timeProfile.get(type) / 1000; // us to ms
    		  if (Math.abs(val - tmp_stat.get(loc).avg) < 2 * Math.sqrt(tmp_stat.get(loc).var)) {
    			  final_stat.get(loc).addValue(val);
    			  avgTime += val;
    			  totalSize++;
    		  }
    	  }
      }
      for (String key : final_stat.keySet()) {
    	  final_stat.get(key).computeAvg();
      }        
      // Compute variance.
      for (EVStatistics evstat : evStatsSet) {
    	  for (StatsType type : evstat.timeProfile.keySet()) {
    		  String loc = GetFolderFromFullPath(type.value);
    		  long val = evstat.timeProfile.get(type) / 1000; // us to ms
    		  if (Math.abs(val - tmp_stat.get(loc).avg) < 2 * Math.sqrt(tmp_stat.get(loc).var))
    			  final_stat.get(loc).addDiff(val);
    	  }
      }
      for (String key : final_stat.keySet()) {
    	  final_stat.get(key).computeVar();
    	  LOG.info(key + "\tavg = " + final_stat.get(key).avg + "  var = " + final_stat.get(key).var + " count = " +
    			  final_stat.get(key).count);
      }
      
      // Set average values.
      if(totalSize > 0) {
    	  evStats.addAggreStat("time_per_record", String.valueOf(avgTime / totalSize));
    	  // We need actualSize rather than the size after removing outlier.
    	  evStats.addAggreStat("total_size", String.valueOf(actualSize));
      } else {
    	  evStats.addAggreStat("time_per_record", String.valueOf(0));
    	  evStats.addAggreStat("total_size", String.valueOf(0));
      }
      // Clear the processed evStats.
      evStatsSet.clear();
      
      return final_stat;
  }
  
  /**
   * Adds the results of Reducer like value and variance into local stats.
   * @param final_val
   * @param final_var
   */
  public void addReduceResults(ArrayList<Double> final_val, ArrayList<Double> final_var) {
	  reduceResults.add(final_val);
	  reduceResults.add(final_var);
  }
  
  public enum OpType {AVG, COUNT, SUM}
  
  private double[] processReduceResults(long n, long N, OpType op) {
	  if (op == OpType.AVG && reduceResults.size() > 1) {
		  double final_sum = 0;
		  double final_var = 0;
		  for (int i=0; i<reduceResults.get(0).size(); i++) {
			  final_sum += reduceResults.get(0).get(i);
			  final_var += reduceResults.get(1).get(i);
		  }
		  // Normal distribution: 1.65 std err = 90%; 1.96 std err = 95%; 2.58 std err = 99%;
		  double error = Math.sqrt(final_var) * 1.96; 
		  // General distribution (Chebyshev's inequality): 
		  // Pr(|X - E(X)| >= a) <= Var(X) / a^2
		  //double error = Math.sqrt(final_var / 0.05); 
		  return new double[]{final_sum, error};
	  }
	  return new double[] {0.0, 0.0};
  }
  
  /**
   * 
   * A file status list class
   * @author fan
   *
   */
  class FileStatusList
  {
	  public List<FileStatus> mlist;
	  
	  public FileStatusList(FileStatus fs)
	  {
		  mlist = new ArrayList<FileStatus>();
		  mlist.add(fs);
	  }
	  
	  public void Add(FileStatus fs)
	  {
		  mlist.add(fs);
	  }
	  
	  public void Sort()
	  {
		  Collections.sort(mlist, new FileComparator());
	  }
	  
	  public class FileComparator implements Comparator<FileStatus>
	  {
		  public int compare(FileStatus f1, FileStatus f2)
		  {
			  String fs1 = f1.getPath().toString();
			  String fs2 = f2.getPath().toString();
			  long fn1 = Long.parseLong(fs1.substring(fs1.lastIndexOf("/")+1, fs1.lastIndexOf(".")));
			  long fn2 = Long.parseLong(fs2.substring(fs2.lastIndexOf("/")+1, fs2.lastIndexOf(".")));
			  long diff = fn1 - fn2;
			  return (int)diff;
		  }
	  }
  }
  
  /**
   * Reorganize the input files
   * @param input
   * @return a map of (folder, sorted filename list)
   */
  public Map<String, FileStatusList> ReorganizeInput(List<FileStatus> input)
  {
	  Map<String, FileStatusList> retmap = new HashMap<String, FileStatusList>();
	  for(FileStatus fs : input)
	  {
		  String folder = DirUtil.GetLast2ndSeg(fs.getPath().toString());
		  FileStatusList fsl = retmap.get(folder);
		  if (fsl == null)
		  {
			  FileStatusList newfsl = new FileStatusList(fs);
			  retmap.put(folder, newfsl);
		  }
		  else
		  {
			  fsl.Add(fs);
		  }
	  }
	  for (FileStatusList value : retmap.values())
	  {
		  value.Sort();
	  }
	  return retmap;
  }
  
//  public List<FileStatus> CacheFilter(List<FileStatus> input)
//  {
//  }
  
  /**
   * Preprocess input list, input reordering, cache hitting and cached result convey
   * @param input
   */
  public void InputPreproc(List<FileStatus> input)
  {
	  Map<String, FileStatusList> inputmap = ReorganizeInput(input);
  }
  
}
