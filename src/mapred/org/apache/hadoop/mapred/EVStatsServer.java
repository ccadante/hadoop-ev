package org.apache.hadoop.mapred;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ClosedByInterruptException;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.EVStatistics;

public class EVStatsServer implements Runnable {
	public static final Log LOG = LogFactory.getLog(EVStatsServer.class);
	
	JobTracker jobTracker;	
	ServerSocket serverSocket;
	boolean running;
	protected Thread listThread;
	
	/**
	 * Process input from socket stream
	 * @author xinfengli
	 *
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
				BufferedReader ipt = new BufferedReader(new InputStreamReader(
						socket.getInputStream()));
				int stats_size = Integer.valueOf(ipt.readLine());
				EVStatistics evStat = new EVStatistics();
				String stat_piece;
				while((stat_piece = ipt.readLine()) != null){
					String[] contents = stat_piece.split(";");
					if (contents.length != 2) {
						LOG.warn("Invalid EVStat format.");
						continue;
					}
					evStat.addTimeStat(contents[0], contents[1]);
				}
				LOG.warn("add EVStat into JobTracker with size = " + evStat.getSize() +
						" value = " + evStat.getFirstStat());
				server.jobTracker.addEVStats(evStat);
			}
			catch (Exception e) {
				LOG.error(e.getMessage());
			}
			finally {
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
	}
	
	public EVStatsServer(int port, JobTracker tracker){
		try {
			serverSocket = new ServerSocket(port);
			running = true;
			jobTracker = tracker;
		}
		catch (Exception e) {
			LOG.error("Init failed: " + e.getMessage());			
		}
	}

	@Override
	public void run() {
		while (running) {
			Socket skt = null;
			try {
				skt = serverSocket.accept();
				if (skt != null)
					new Thread(new DataProcess(this, skt)).start();
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
		LOG.info("Server started.");
		return running;
	}
	
	public void stop() {
		running = false;
		if (listThread.getState() == Thread.State.BLOCKED)
			listThread.interrupt();
		LOG.info("Server stopped.");
	}
}
