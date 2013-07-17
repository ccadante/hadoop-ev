package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.mortbay.log.Log;

public class CombineSampleInputFormat extends FileInputFormat<Text, BytesWritable> {
	public static final String SPLIT_MINSIZE_PERNODE = 
			"mapreduce.input.fileinputformat.split.minsize.per.node";
	public static final String SPLIT_MINSIZE_PERRACK = 
			"mapreduce.input.fileinputformat.split.minsize.per.rack";
	// ability to limit the size of a single split
	private long maxSplitSize = 0;
	private long minSplitSizeNode = 0;
	private long minSplitSizeRack = 0;

	// A pool of input paths filters. A split cannot have blocks from files  
	// across multiple pools.
	private ArrayList<MultiPathFilter> pools = new  ArrayList<MultiPathFilter>();


	// mapping from a rack name to the set of Nodes in the rack 
	private HashMap<String, Set<String>> rackToNodes = 
			new HashMap<String, Set<String>>();
	
	/**
	 * Specify the maximum size (in bytes) of each split. Each split is
	 * approximately equal to the specified size.
	 */
	protected void setMaxSplitSize(long maxSplitSize) {
	    this.maxSplitSize = maxSplitSize;
	}

	/**
	 * Specify the minimum size (in bytes) of each split per node.
	 * This applies to data that is left over after combining data on a single
	 * node into splits that are of maximum size specified by maxSplitSize.
	 * This leftover data will be combined into its own split if its size
	 * exceeds minSplitSizeNode.
	 */
	protected void setMinSplitSizeNode(long minSplitSizeNode) {
	    this.minSplitSizeNode = minSplitSizeNode;
	}

	/**
	 * Specify the minimum size (in bytes) of each split per rack.
	 * This applies to data that is left over after combining data on a single
	 * rack into splits that are of maximum size specified by maxSplitSize.
	 * This leftover data will be combined into its own split if its size
	 * exceeds minSplitSizeRack.
	 */
	protected void setMinSplitSizeRack(long minSplitSizeRack) {
	    this.minSplitSizeRack = minSplitSizeRack;
	}

	/**
	 * Create a new pool and add the filters to it.
	 * A split cannot have files from different pools.
	 */
	protected void createPool(List<PathFilter> filters) {
		pools.add(new MultiPathFilter(filters));
	}

	/**
	 * Create a new pool and add the filters to it. 
	 * A pathname can satisfy any one of the specified filters.
	 * A split cannot have files from different pools.
	 */
	protected void createPool(PathFilter... filters) {
		MultiPathFilter multi = new MultiPathFilter();
	    for (PathFilter f: filters) {
	      multi.add(f);
	    }
	    pools.add(multi);
	}
	  

	@Override 
	protected boolean isSplitable(JobContext context, Path filename)
	{
		return true;
	}

	/**
	 * default constructor
	 */
	public CombineSampleInputFormat() 
	{
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) 
			throws IOException {

		long minSizeNode = 0;
	    long minSizeRack = 0;
	    long maxSize = 0;
	    Configuration conf = job.getConfiguration();

	    // the values specified by setxxxSplitSize() takes precedence over the
	    // values that might have been specified in the config
	    if (minSplitSizeNode != 0) {
	    	minSizeNode = minSplitSizeNode;
	    } else {
	    	minSizeNode = conf.getLong(SPLIT_MINSIZE_PERNODE, 0);
	    }
	    if (minSplitSizeRack != 0) {
	    	minSizeRack = minSplitSizeRack;
	    } else {
	    	minSizeRack = conf.getLong(SPLIT_MINSIZE_PERRACK, 0);
	    }
	    if (maxSplitSize != 0) {
	    	maxSize = maxSplitSize;
	    } else {
	    	maxSize = conf.getLong("mapreduce.input.fileinputformat.split.maxsize", 0);
	    }
	    if (minSizeNode != 0 && maxSize != 0 && minSizeNode > maxSize) {
	    	throw new IOException("Minimum split size pernode " + minSizeNode +
	    			" cannot be larger than maximum split size " + maxSize);
	    }
	    if (minSizeRack != 0 && maxSize != 0 && minSizeRack > maxSize) {
	    	throw new IOException("Minimum split size per rack" + minSizeRack +
	    			" cannot be larger than maximum split size " + maxSize);
	    }
	    if (minSizeRack != 0 && minSizeNode > minSizeRack) {
	    	throw new IOException("Minimum split size per node" + minSizeNode +
	    			" cannot be smaller than minimum split " + "size per rack " + minSizeRack);
	    }

	    Log.info("$$$$$$$$$$$$$  max size = " + maxSize);
	    
	    // all the files in input set
	    SamplePath[] paths = SampleInputUtil.getInputSamplePaths(job);
	    List<InputSplit> splits = new ArrayList<InputSplit>();
	    if (paths.length == 0) {
	    	return splits;    
	    }

	    // Convert them to Paths first. This is a costly operation and 
	    // we should do it first, otherwise we will incur doing it multiple
	    // times, one time each for each pool in the next loop.
	    List<SamplePath> newpaths = new LinkedList<SamplePath>();
	    for (int i = 0; i < paths.length; i++) {
	    	Path p = new Path(paths[i].file_path.toUri().getPath());
	    	newpaths.add(new SamplePath(p, paths[i].sample_key, paths[i].size));
	    }
	    paths = null;

	    // In one single iteration, process all the paths in a single pool.
	    // Processing one pool at a time ensures that a split contains paths
	    // from a single pool only.
    	ArrayList<SamplePath> myPaths = new ArrayList<SamplePath>();
      
    	// pick one input path. If it matches all the filters in a pool,
    	// add it to the output set
    	for (Iterator<SamplePath> iter = newpaths.iterator(); iter.hasNext();) {
    		SamplePath p = iter.next();
			myPaths.add(p); // add it to my output set
			iter.remove();
    	}
    	// create splits for all files in this pool.
    	getMoreSplits(job, myPaths.toArray(new SamplePath[myPaths.size()]), 
                    maxSize, minSizeNode, minSizeRack, splits);

	    // free up rackToNodes map
	    rackToNodes.clear();
	    return splits;    
	}

	/**
	 * Return all the splits in the specified set of paths
	 */
	private void getMoreSplits(JobContext job, SamplePath[] paths, 
	                           long maxSize, long minSizeNode, long minSizeRack,
	                           List<InputSplit> splits)
	    throws IOException {
	    Configuration conf = job.getConfiguration();

	    if (paths.length == 0) {
	    	return; 
	    }

	    ArrayList<SamplePath> validPaths = new ArrayList<SamplePath>();
	    long curSplitSize = 0;

	    for (SamplePath sp : paths)
	    {
	    	validPaths.add(sp);
	    	curSplitSize += sp.size;
	    	if (maxSize != 0 && curSplitSize >= maxSize)
	    	{
	  	      	addCreatedSplit(splits, validPaths);
	  	      	validPaths.clear();
	  	      	curSplitSize = 0;
	    	}
	    }
	    addCreatedSplit(splits, validPaths);
	    validPaths.clear();
	}

	private void addCreatedSplit(List<InputSplit> splitList, ArrayList<SamplePath> validPaths) {
		CombineSampleSplit thissplit = new CombineSampleSplit(validPaths.toArray(new SamplePath[validPaths.size()]));
	    splitList.add(thissplit); 
	}
	
	@SuppressWarnings("finally")
	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) 
			throws IOException
	{
//		CombineFileRecordReader<Text, BytesWritable> reader = new CombineFileRecordReader<Text, BytesWritable>(
//				(CombineFileSplit)split, context, CombineSampleRecordReader.class);
		CombineSampleRecordReader reader = null;
		try {
			reader = new CombineSampleRecordReader(split, context);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			return reader;
		}
	}
	  
	
	
	
	
	/**
	 * Accept a path only if any one of filters given in the
	 * constructor do. 
	 */
	private static class MultiPathFilter implements PathFilter {
	    private List<PathFilter> filters;
	
	    public MultiPathFilter() {
	    	this.filters = new ArrayList<PathFilter>();
	    }
	
	    public MultiPathFilter(List<PathFilter> filters) {
	    	this.filters = filters;
	    }
	
	    public void add(PathFilter one) {
	    	filters.add(one);
	    }
	
	    public boolean accept(Path path) {
	    	for (PathFilter filter : filters) {
	    		if (filter.accept(path)) {
	    			return true;
	    		}
	    	}
	    	return false;
	    }
	
	    public String toString() {
	    	StringBuffer buf = new StringBuffer();
	    	buf.append("[");
	    	for (PathFilter f: filters) {
	    		buf.append(f);
	    		buf.append(",");
	    	}
	    	buf.append("]");
	    	return buf.toString();
	    }
	}
	  

}