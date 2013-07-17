package org.apache.hadoop.mapreduce.lib.input;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.util.StringUtils;
import org.mortbay.log.Log;

public class SampleInputUtil {
	public static final String SAMP_DIR = "mapred.input.sample.dir";
	
	public static SamplePath[] getInputSamplePaths(JobContext context) {
	    String dirs = context.getConfiguration().get(SAMP_DIR, "");
	    String [] list = StringUtils.split(dirs);
	    SamplePath[] result = new SamplePath[list.length];
	    for (int i = 0; i < list.length; i++) {
	    	String[] file_key_size = list[i].split(":");
	    	result[i] = new SamplePath(new Path(file_key_size[0]), file_key_size[1], Long.parseLong(file_key_size[2]));
	    }
	    return result;
	}
}
