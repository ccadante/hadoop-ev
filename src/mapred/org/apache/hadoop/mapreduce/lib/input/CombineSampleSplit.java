package org.apache.hadoop.mapreduce.lib.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapFileSampleProc;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CombineSampleSplit extends InputSplit implements Writable {
	public static final Log LOG = LogFactory.getLog(CombineSampleSplit.class);

	private SamplePath[] paths;
	private long totLength;

	/**
	 * default constructor
	 */
	public CombineSampleSplit() {}
	
	public CombineSampleSplit(SamplePath[] files)
	{
		initSplit(files);
	}
  	
  	private void initSplit(SamplePath[] files) 
  	{
  		this.paths = files;
  		this.totLength = 0;
  		for(SamplePath sp : files) {
  			totLength += sp.size;
  		}
  	}

  	/**
  	 * Copy constructor
  	 */
  	public CombineSampleSplit(CombineSampleSplit old) throws IOException {
  		this(old.getPaths());
  	}

  	@Override
  	public long getLength() {
  		return totLength;
  	}

  	@Override
  	public String[] getLocations()
  	{
  		String[] ret = {"localhost"};
  		return ret;
  	}
  	
  	/** Returns the number of Paths in the split */
  	public int getNumPaths() {
  		return paths.length;
  	}
  	

  	/** Returns the i<sup>th</sup> Path */
  	public SamplePath getPath(int i) {
  		return paths[i];
  	}
  
  	/** Returns all the Paths in the split */
  	public SamplePath[] getPaths() {
  		return paths;
  	}

  	public void readFields(DataInput in) throws IOException {
  		totLength = in.readLong();
  		int arrLength = in.readInt();
  		paths = new SamplePath[arrLength];
  		for(int i=0; i<arrLength;i++) {
  			String strin = Text.readString(in);
  			//LOG.info("@@@@@@ + " + strin);	
  			String[] file_key_size = strin.split(":");
  			paths[i] = new SamplePath(new Path(file_key_size[0]), file_key_size[1], Long.parseLong(file_key_size[2]));
  		}
  	}

  	public void write(DataOutput out) throws IOException {
  		out.writeLong(totLength);
  		out.writeInt(paths.length);
  		for(SamplePath p : paths) {
  			Text.writeString(out, p.file_path.toString() + ":" + p.sample_key + ":" + p.size);
  		}
  	}
  
  	@Override
  	public String toString() {
  		StringBuffer sb = new StringBuffer();
  		for (int i = 0; i < paths.length; i++) {
  			if (i == 0 ) {
  				sb.append("Paths:");
  			}
  			sb.append(paths[i].file_path.toUri().getPath() + ":" + paths[i].sample_key +
  					  ":" + paths[i].size);
  			if (i < paths.length -1) {
  				sb.append(",");
  			}
  		}
  		return sb.toString();
  	}
}

