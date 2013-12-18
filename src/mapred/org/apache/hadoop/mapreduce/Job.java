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
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.EVStatistics.Stats;
import org.apache.hadoop.mapreduce.EVStatistics.StatsType;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.CombineSampleInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFilter;
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

  public Map<String, Double> sampledSize = new HashMap<String, Double>();
  //A integer indicating the number of DataProcesses in EVStatsServer
  //public int dpInProc = 0;
  
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
   * Specify whether job-setup and job-cleanup is needed for the job 
   * 
   * @param needed If <code>true</code>, job-setup and job-cleanup will be
   *               considered from {@link OutputCommitter} 
   *               else ignored.
   */
  public void setJobSetupCleanupNeeded(boolean needed) {
    ensureState(JobState.DEFINE);
    conf.setBoolean("mapred.committer.job.setup.cleanup.needed", needed);
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
 * @throws IllegalAccessException 
 * @throws InstantiationException 
   */
  public boolean waitForSampleCompletion() throws IOException, InterruptedException, ClassNotFoundException, CloneNotSupportedException, InstantiationException, IllegalAccessException
  {
	  LOG.info("@@@@@ input format class @@@@@: " + this.getInputFormatClass());
	  /* If the input class is SequenceFileInputFilter */
	  if (this.getInputFormatClass().equals(SequenceFileInputFilter.class))
	  {
		  LOG.info("@@@@@ sequence file branch @@@@@");
		  SequenceFileSampleProc sfsp = new SequenceFileSampleProc();
		  return sfsp.setup(this) && sfsp.start(); // sfsp.startWithErrorContraint();
	  }
	  /* If the input class is CombineSampleInputFormat */
	  if (this.getInputFormatClass().equals(CombineSampleInputFormat.class))
	  {
		  LOG.info("@@@@@ combine sample branch @@@@@");
		  MapFileSampleProc mfsp = new MapFileSampleProc();
		  return mfsp.setup(this) && mfsp.start(); // mfsp.startWithErrorContraint();
	  }	  
	  
	  LOG.error("Invalid input format (MapFile or SequenceFile). Exit!");
	  return true;
  }
  
  // Get the folder name from a full path name, which is the deepest directory name.
  public static String GetFolderFromFullPath(String path) {
	  if (!path.contains("/"))
		  return path;
	  String folder = null;
	  try {
		  folder = path;
		  folder = folder.substring(0, folder.lastIndexOf("/"));
		  folder = folder.substring(folder.lastIndexOf("/")+1);	
		  folder = folder + "/1";
	  } catch (Exception ex) {
		  LOG.error("GetFolderFromFullPath:" + ex.getMessage());
	  }
	  return folder;
  }
 
  // The size of evStatsSet should equal number of Map tasks.
  Set<EVStatistics> evStatsSet = new HashSet<EVStatistics>();
  EVStatistics evStats = new EVStatistics();
  // format <loc, {{reduce_avgVal,...}, {reduce_var,...}, {reduce_valCount,...}}>
  Map<String, ArrayList<ArrayList<Double>>> reduceResults = new HashMap<String, ArrayList<ArrayList<Double>>>(); 
  ArrayList<ArrayList<Double>> reducerTimes = new ArrayList<ArrayList<Double>>(); 
  ArrayList<ArrayList<Double>> mapperTimes = new ArrayList<ArrayList<Double>>(); 
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
	  synchronized (evStatsSet){
		  evStatsSet.add(evStat);
		  //LOG.warn("addEVStats set size = " + evStatsSet.size());
	  }
  }
  
  public void clearEVStats() {
	  LOG.info("Clear all EVStats!");
	  evStatsSet.clear();
	  evStats.clear();
	  reduceResults.clear();
	  reducerTimes.clear();
	  mapperTimes.clear();
  }
  /**
   * Process current set of EVStats to get the time-cost distribution across different domain. 
   * Currently, we aggregate data for each folder (i.e, camera location).
   * @return Map<String, Stats>, e.g., <"16m_1", <avg=1.0, var=0.90>>, <"16m_1", <avg=2.0, var=4.90>>
   */
  public Map<String, Stats> processEVStats(){	  
	  LOG.info("processEVStats");
	  long actualSize = 0;
	  Map<String, Stats> tmp_stat = new HashMap<String, Stats>();
	  Map<String, Stats> final_stat = new HashMap<String, Stats>();
	  
	  // Compute time average.
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
      // Compute time variance.
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
      // Compute time average.
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
    		  if (Math.abs(val - tmp_stat.get(loc).getAvg()) < 2 *tmp_stat.get(loc).getStd()) {
    			  final_stat.get(loc).addValue(val);
    			  avgTime += val;
    			  totalSize++;
    		  }
    	  }
      }
      for (String key : final_stat.keySet()) {
    	  final_stat.get(key).computeAvg();
      }        
      /*// Compute time variance.
      for (EVStatistics evstat : evStatsSet) {
    	  for (StatsType type : evstat.timeProfile.keySet()) {
    		  String loc = GetFolderFromFullPath(type.value);
    		  long val = evstat.timeProfile.get(type) / 1000; // us to ms
    		  if (Math.abs(val - tmp_stat.get(loc).avg) < 2 * Math.sqrt(tmp_stat.get(loc).var))
    			  final_stat.get(loc).addDiff(val);
    	  }
      }*/
      // Process values variance
      double var_avg = 0.0; // just for key with zero variance stats.
      double var_avg_count = 0.0;
      for (String key : reduceResults.keySet()) {
		  ArrayList<ArrayList<Double>> lists = reduceResults.get(key);		
		  ArrayList<Double> val_list = lists.get(0);
		  ArrayList<Double> var_list = lists.get(1);
		  ArrayList<Double> count_list = lists.get(2);
		  double val = 0.0;
		  double var = 0.0;
		  double count = 0.0;
		  for (int i = 0; i < count_list.size(); i++) {
			  count += count_list.get(i);			  
		  }
		  for (int i = 0; i < val_list.size(); i++) {
			  val += val_list.get(i) * count_list.get(i) / count; // weighted average value			  
		  }
		  for (int i = 0; i < var_list.size(); i++) {
			  var += var_list.get(i) * Math.pow(count_list.get(i) / count, 2); // weighted variance			  
		  }		  
		  // update reduceResults
		  val_list.clear();
		  val_list.add(val);
		  var_list.clear();
		  var_list.add(var);
		  count_list.clear();
		  count_list.add(count);
		  
		  var_avg += var;
		  var_avg_count++;
		 // LOG.info("reduceResults.keySet: "+key+"\t"+var);
		  if (final_stat.containsKey(key)) {
			  final_stat.get(key).setVar(var);
			  final_stat.get(key).setCount((long) count);
		  }
	  }
      
      if (var_avg_count > 0) {
    	  var_avg = var_avg / (double) var_avg_count;
      }
      // To avoid 0.0 variance.
      if (var_avg < 0.0001){
    	  var_avg = 0.01;
      }
      // Set variances of other non-existing keys 
      for (String key : final_stat.keySet()) {
    	  if (final_stat.get(key).getVar() < 0.0001) {
    		  final_stat.get(key).setVar(var_avg);
    	  }
      }
      
      for (String key : final_stat.keySet()) {
    	  LOG.info("#########  " + key + "    avg(Time) = " + String.format("%.3f", final_stat.get(key).getAvg())
    			  + "  var(Value) = " + String.format("%.3f", final_stat.get(key).getVar())
    			  + "  size = " + final_stat.get(key).getCount());
      }
      
      // Process mapper_time and reducer_time
      Long firstMapperTime = Long.MAX_VALUE;
      Long lastMapperTime = Long.MIN_VALUE;
      for (ArrayList<Double> times : mapperTimes) {
    	  firstMapperTime = Math.min(firstMapperTime, Math.round(times.get(0)));
    	  lastMapperTime = Math.max(lastMapperTime, Math.round(times.get(0) + times.get(1)));
      }      
      Long avgReducerTime = 0L;
      for (ArrayList<Double> times : reducerTimes) {
    	  avgReducerTime += Math.round(times.get(1));
      }
      if (reducerTimes.size() > 0)
    	  avgReducerTime = avgReducerTime / reducerTimes.size();
      else
    	  LOG.error("Invalid reducerTimes (probably also mapperTimes)!");
      // Do remember to clear mapperTimes and reducerTimes every time!
      mapperTimes.clear();
      reducerTimes.clear();
      
      // Set evStats values.
      if(totalSize > 0) {
    	  evStats.addAggreStat("time_per_record", String.valueOf(avgTime / totalSize));
    	  // We need actualSize rather than the size after removing outlier.
    	  evStats.addAggreStat("total_size", String.valueOf(actualSize));
    	  evStats.addAggreStat("first_mapper_time", String.valueOf(firstMapperTime));
    	  evStats.addAggreStat("last_mapper_time", String.valueOf(lastMapperTime));
    	  evStats.addAggreStat("avg_reducer_time", String.valueOf(avgReducerTime));
    	  LOG.info("map_startTime = " + firstMapperTime + "  map_endTime = " + lastMapperTime
    			  + "  " + "map_phase = " + (lastMapperTime - firstMapperTime) + 
    			  "ms  avgReducerTime = " + avgReducerTime + "ms");
      } else {
    	  evStats.addAggreStat("time_per_record", String.valueOf(0));
    	  evStats.addAggreStat("total_size", String.valueOf(0));
    	  evStats.addAggreStat("first_mapper_time", String.valueOf(0));
    	  evStats.addAggreStat("last_mapper_time", String.valueOf(lastMapperTime));
    	  evStats.addAggreStat("avg_reducer_time", String.valueOf(0));
    	  LOG.info("firstMapperTime = " + 0 + "  lastMapperTime = " + 0 + "  " +
    			  "whole map phase = " + (0 - 0) + 
    			  "ms  avgReducerTime = " + 0 + "ms");
      }
      // Clear the processed evStats.
      //evStatsSet.clear();
      
      return final_stat;
  }
  
  /**
   * Adds the results of Reducer like value and variance into local stats.
   * 
   */
  public void addReduceResults(ArrayList<String> final_keys, ArrayList<Double> final_val,
		  ArrayList<Double> final_var, ArrayList<Double> final_count) {
	  synchronized (reduceResults) {
		  for (int i=0; i<final_keys.size(); i++) {		  
			  String key = final_keys.get(i);
			  /*LOG.info("addReduceResults: " + key
					  + "  val = " + final_val.get(i)
					  + "  var = " + final_var.get(i)
					  + "  size = " + final_count.get(i));*/
			  if (!reduceResults.containsKey(key)) {
				  ArrayList<Double> val_list = new ArrayList<Double>();
				  val_list.add(final_val.get(i));
				  ArrayList<Double> var_list = new ArrayList<Double>();
				  var_list.add(final_var.get(i));
				  ArrayList<Double> count_list = new ArrayList<Double>();
				  count_list.add(final_count.get(i));
				  ArrayList<ArrayList<Double>> newList = new ArrayList<ArrayList<Double>>();
				  newList.add(val_list);
				  newList.add(var_list);
				  newList.add(count_list);
				  reduceResults.put(key, newList);
				  continue;
			  }
			  ArrayList<ArrayList<Double>> oldList = reduceResults.get(key);
			  oldList.get(0).add(final_val.get(i));
			  oldList.get(1).add(final_var.get(i));
			  oldList.get(2).add(final_count.get(i));
		  }
	  }
  }
  
  public void addReduceTime(ArrayList<Double> reducer_time) {
	  synchronized (reducerTimes){
		  reducerTimes.add(reducer_time);
	  }
  }
  
  public void addMapperTime(ArrayList<Double> mapper_time) {
	  synchronized (mapperTimes){
		  mapperTimes.add(mapper_time);
	  }
  }
  
  public enum OpType {AVG, COUNT, SUM}
  
  public double[] processReduceResults(long n, long N, OpType op) {
	  if (op == OpType.SUM) {
		  LOG.info("processReduceResults: Sample Size = " + n + "  Total size = " + N);
		  ArrayList<String> emptyKeys = new ArrayList<String>();
		  ArrayList<String> nonEmptyKeys = new ArrayList<String>();
		  double final_sum = 0;
		  double final_var = 0;
		  for (String key : reduceResults.keySet()) {
			  ArrayList<ArrayList<Double>> lists = reduceResults.get(key);		
			  ArrayList<Double> val_list = lists.get(0);
			  ArrayList<Double> var_list = lists.get(1);
			  ArrayList<Double> count_list = lists.get(2);
			  double val = 0.0;
			  double var = 0.0;
			  double count = 0.0;
			  for (int i = 0; i < count_list.size(); i++) {
				  count += count_list.get(i);			  
			  }
			  for (int i = 0; i < val_list.size(); i++) {
				  double weight = count_list.get(i) / count;
				  val += val_list.get(i) * weight;			  
			  }
			  double val_2 = 0; // E(x^2)
			  for (int i = 0; i < var_list.size(); i++) {
				  double weight = count_list.get(i) / count;
				  // E(x^2) = var + (E(x))^2
				  val_2 += weight * (var_list.get(i) + Math.pow(val_list.get(i), 2));
			  }		
			  // var = E(x^2) - (E(x))^2
			  var = val_2 - Math.pow(val, 2);
			  // update reduceResults
			  val_list.clear();
			  val_list.add(val);
			  var_list.clear();
			  var_list.add(var);
			  count_list.clear();
			  count_list.add(count);
			  
			  LOG.info("#########  " + key + "    val(Value) = " + String.format("%.3f", val)
					  + "  var(Value) = " + String.format("%.3f", var)
					  + "  size = " + count);
			  
			  final_sum += val;
			  final_var += var;			  			  
			  if (val < 0.001) {
				  emptyKeys.add(key);
			  } else {
				  nonEmptyKeys.add(key);
			  }
		  }
		  // Clear reduce results of this round.
		  //reduceResults.clear();
		  if (this.getConfiguration().getBoolean("mapred.sample.printEmptyFolder", false)) {
			  String nonEmptyKeysStr = "";
			  for (String key : nonEmptyKeys) {
				  nonEmptyKeysStr += key + "\n";
			  }
			  LOG.warn("Folder list with valid computing results:\n" + nonEmptyKeysStr);
			  String emptyKeysStr = "";
			  for (String key : emptyKeys) {
				  emptyKeysStr += key + "\n";
			  }
			  LOG.warn("Folder list with ZERO computing results:\n" + emptyKeysStr);
		  }
		  // Normal distribution: 1.65 std err = 90%; 1.96 std err = 95%; 2.58 std err = 99%;
		  double error = Math.sqrt(final_var) * 1.96; 
		  // General distribution (Chebyshev's inequality): 
		  // Pr(|X - E(X)| >= a) <= Var(X) / a^2
		  //double error = Math.sqrt(final_var / 0.05); 
		  return new double[]{final_sum, error};
	  }
	  return new double[] {-1.0, 0.0};
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
  
  /**
   * Preprocess input list, input reordering, cache hitting and cached result convey
   * @param input
   */
  public void InputPreproc(List<FileStatus> input)
  {
	  Map<String, FileStatusList> inputmap = ReorganizeInput(input);
  }
  
}
