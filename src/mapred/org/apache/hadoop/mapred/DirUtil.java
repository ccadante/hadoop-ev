package org.apache.hadoop.mapred;

/**
 * Process the directory string(slash-split) to get segments, 
 * e.g. filename(the last segment) or folder name
 * @author fan
 *
 */
public class DirUtil {

	public static String GetLastSeg(String dir)
	{
		if(dir.lastIndexOf("/") == dir.length()-1)
			dir = dir.substring(0, dir.length()-1);
		return dir.substring(dir.lastIndexOf("/") + 1);
	}
	
	public static String GetLast2ndSeg(String dir)
	{
		if(dir.lastIndexOf("/") == dir.length()-1)
			dir = dir.substring(0, dir.length()-1);
		dir = dir.substring(0, dir.lastIndexOf("/"));
		return dir.substring(dir.lastIndexOf("/") + 1);
	}
}
