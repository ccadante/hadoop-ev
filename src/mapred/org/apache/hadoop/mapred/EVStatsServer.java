package org.apache.hadoop.mapred;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ClosedByInterruptException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.EVStatistics;
import org.apache.hadoop.mapreduce.Job;

public class EVStatsServer implements Runnable {
	public static final Log LOG = LogFactory.getLog(EVStatsServer.class);
	
	FileSystem hdfs;
	public String cache_prefix;
	Map<String, FSDataOutputStream> cache_file_map;
	
	JobTracker jobTracker;	
	Job job;
	ServerSocket serverSocket;
	int port;
	boolean running;
	protected Thread listThread;
	
	/**
	 * Process input from socket stream
	 * @author xinfengli
	 */
	class DataProcess implements Runnable {
		EVStatsServer server;
		Socket socket;
		
		public DataProcess(EVStatsServer svr, Socket skt){
			server = svr;
			socket = skt;
		}
		
		@Override
		public void run() {
			try {
				/*synchronized(this) {
					job.dpInProc += 1;
				}*/
				BufferedReader ipt = new BufferedReader(new InputStreamReader(
						socket.getInputStream()));
				int state = Integer.valueOf(ipt.readLine());
				int data_size = Integer.valueOf(ipt.readLine());
				EVStatistics evStat = null;
				ArrayList<String> final_keys = null;
				ArrayList<Double> final_val = null;
				ArrayList<Double> final_var = null;
				ArrayList<Double> final_count = null;
				ArrayList<Double> reducer_time = null;
				ArrayList<Double> mapper_time = null;
				//LOG.info("Statistics built, state = " + state + "; data size = " + data_size);
				if (state == 0) { // from Mapper
					evStat = new EVStatistics();
					mapper_time = new ArrayList<Double>();
				}
				else if (state == 1) { // from Reducer
					final_keys = new ArrayList<String>();
					final_val = new ArrayList<Double>();
					final_var = new ArrayList<Double>();
					final_count = new ArrayList<Double>();
					reducer_time = new ArrayList<Double>();
				}
				String stat_piece;
				while((stat_piece = ipt.readLine()) != null){
					//LOG.info("stat: " + stat_piece);
					if (state == 0) { // EVStat data and Cache data
						String[] contents = stat_piece.split(";");
						if (Integer.parseInt(contents[0]) == 0)	// EVStat data
							evStat.addTimeStat(contents[1], contents[2]);
						else if (Integer.parseInt(contents[0]) == 1)  // Cache data
							evStat.addCacheItem(contents[1], contents[2]);
						else if (Integer.parseInt(contents[0]) == 2) // Mapper times
						{
							mapper_time.add(Double.parseDouble(contents[1]));
							mapper_time.add(Double.parseDouble(contents[2]));
						}
					} else if (state == 1) { // Reducer
						String[] contents = stat_piece.split(";");
						if (Integer.parseInt(contents[0]) == 0)	// Reducer results
						{
							final_keys.add(contents[1]);
							final_val.add(Double.parseDouble(contents[2]));
							final_var.add(Double.parseDouble(contents[3]));
							final_count.add(Double.parseDouble(contents[4]));
						} else if (Integer.parseInt(contents[0]) == 1)  // Reducer time
						{
							reducer_time.add(Double.parseDouble(contents[1]));
							reducer_time.add(Double.parseDouble(contents[2]));
						}						
					}
				}
				if (state == 0) {
					server.job.addEVStats(evStat);
					storeCache(evStat);
//					closeHDFS();
					//server.jobTracker.addEVStats(evStat);
					server.job.addMapperTime(mapper_time);
//					LOG.warn("Added EVStat(Map) into Job with size = " + evStat.getSize() +
//							"  and one time = " + evStat.getFirstStat() + "  mapper_time = " + mapper_time + "ms");
				} else if (state == 1) {
					server.job.addReduceResults(final_keys, final_val, final_var, final_count);
					server.job.addReduceTime(reducer_time);
//					LOG.warn("Added ReduceResult into Job with size = " + final_val.size() +
//							" and one value = " + final_val.get(0) + "variance = " + final_var.get(0) + "  reducer_time = " + reducer_time + "ms");
				}
			}
			catch (Exception e) {
				LOG.error(e.getMessage());
			}
			finally {
				/*synchronized(this) {
					job.dpInProc -= 1;
				}*/
				try {
					socket.close();
				}
				catch (Exception e) {
					LOG.error(e.getMessage());
				}
				finally {
					socket = null;
					Thread.yield();
				}
			}
		}
		
		/**
		 * Store cache hash map in evStat to HDFS /cache
		 * @throws IOException 
		 */
		public void storeCache(EVStatistics evs) throws IOException
		{
			for (EVStatistics.CacheItem ci : evs.cacheList) {
				String cikey = DirUtil.GetLast2ndSeg(ci.key);
				String content = ci.key + ";" + ci.value + "\n";
//				LOG.info("key value : " + content);
				
				FSDataOutputStream os = cache_file_map.get(cikey);
				if (os == null) {
					os = hdfs.create(new Path(
							job.getConfiguration().get("fs.default.name") + "/cache/" + cikey));
					cache_file_map.put(cikey, os);
					LOG.info("Create cache file: " + 
							job.getConfiguration().get("fs.default.name") + "/cache/" + cikey);
				}
			    os.write(content.getBytes("UTF-8"));
			}
		}
		
		/**
		 * Close all file streams
		 * @throws IOException 
		 */
		public void closeHDFS() throws IOException
		{
			for (Map.Entry<String, FSDataOutputStream> ent : cache_file_map.entrySet())
				ent.getValue().close();
			hdfs.close();
		}
	}
	
	public EVStatsServer(int pt, JobTracker tracker){
		try {
			port = pt;
			serverSocket = new ServerSocket(port);
			running = true;
			jobTracker = tracker;
			LOG.info("EVStatsServer starts in port " + port);
		}
		catch (Exception e) {
			LOG.error("Init failed: " + e.getMessage());			
		}
	}

	public EVStatsServer(int pt, Job jb) {
		try {
			port = pt;
			serverSocket = new ServerSocket(port);
			running = true;
			job = jb;
			InitCacheFileOutput();
			LOG.info("EVStatsServer starts in port " + port);
		}
		catch (Exception e) {
			LOG.error("Init failed: " + e.getMessage());			
		}
	}
	
	/**
	 * Initiate cache file writing streams
	 * @throws IOException
	 */
	public void InitCacheFileOutput() throws IOException
	{
		hdfs = FileSystem.get(job.getConfiguration());
		cache_prefix = job.getConfiguration().get("fs.default.name") + "/cache";
		cache_file_map = new HashMap<String, FSDataOutputStream>();
		
		if(!hdfs.exists(new Path(cache_prefix)))
			hdfs.mkdirs(new Path(cache_prefix));
		FileStatus[] fstats = hdfs.listStatus(new Path(cache_prefix));
		for (int i=0; i<fstats.length; i++) {
			if (hdfs.isFile(fstats[i].getPath())) {
				String fp = fstats[i].getPath().toString();
				FSDataOutputStream os = hdfs.append(new Path(fp));
				String key = DirUtil.GetLastSeg(fp);
				cache_file_map.put(key, os);
			}
		}
	}
	
	@Override
	public void run() {
		while (running) {
			Socket skt = null;
			try {
				skt = serverSocket.accept();
				if (skt != null) {
					Thread dpthd = new Thread(new DataProcess(this, skt));
					dpthd.start();
				}
			}
			catch (ClosedByInterruptException e) {
				LOG.error(e.getMessage());
			} catch (IOException e) {
				LOG.error(e.getMessage());
			}
		}
	}

	public boolean start() {
		if (listThread!=null)
			running = false;		
		listThread = new Thread(this);
		running = true;
		listThread.start();	
		LOG.info("Server started at port " + port);
		return running;
	}
	
	public void stop() {
		running = false;
		if (listThread.getState() == Thread.State.BLOCKED)
			listThread.interrupt();
		LOG.info("Server stopped.");
	}
}
