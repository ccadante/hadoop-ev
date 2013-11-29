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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.EVStatistics.CacheItem;

public class EVStatistics {
	private static final Log LOG = LogFactory.getLog(EVStatistics.class);
	
	public Map<StatsType, Long> timeProfile = new HashMap<StatsType, Long>();
	public Map<String, String> aggreStats = new HashMap<String, String>();
	ArrayList<Double> time_record = new ArrayList<Double>(); // Two items only: startTime, timeCost
	  
	// A cache for a mapper task
	public List<CacheItem> cacheList = new ArrayList<CacheItem>();
	
	// The type of stats, for example, location:1, time:2.
	class StatsType {
		String type;
		String value;
		public StatsType(String t, String v) {
			type = t;
			value = v;
		}
		
		public StatsType(String str) {
			String[] pair = str.split(":");
			if (pair.length == 2) {
				type = pair[0];
				value = pair[1];
			} else {
				type = str;
				value = "UNKNOWN";
			}
		}
		
		public String toString(){
			return (type + ":" + value);
		}
	}
	
	// The real number stats.
	class Stats {
		// NOTE: time in ms.
		double avg;
		double var;
		long count;
		double total;
		double varTotal;
		
		public Stats () {
			avg = var = count = 0;
			total = varTotal = 0.0;
		}
		
		public void addValue(long value) {
			count++;
			total += value;
		}
		
		public void addDiff(long value) {
			varTotal += Math.pow((value - avg), 2);
		}
		
		public void computeAvg() {
			if (count > 0) {
				avg = total / (double) count;
			}
		}
		
		public void computeVar() {
			if (count > 0) {
				var = varTotal / (double) count;
			}
		}
	}
	
	// An (key,value) item in cache
	public class CacheItem {
		public String key;
		public String value;
		
		public CacheItem(String k, String v) {
			key = k;
			value = v;
		}
	}
	
	
	public EVStatistics(){
		timeProfile.clear();
	}
	
	public void clear() {
		timeProfile.clear();
		aggreStats.clear();
	}

	public void addTimeStat(StatsType type, long time) {
		timeProfile.put(type, time);
		//LOG.info("addStat: " + time);
	}
	
	public void addTimeStat(String typeStr, String timeStr) {
		timeProfile.put(new StatsType(typeStr), Long.parseLong(timeStr));
	}
	
	public void addAggreStat(String key, String value) {
		aggreStats.put(key, value);
	}
	
	public String getAggreStat(String key) {
		return aggreStats.get(key);
	}

	public int getSize() {
		return timeProfile.size();
	}

	public long getFirstStat() {
		for (Long value : timeProfile.values()) {
			return value;
		}
		return -1;
	}
	
	public long getAvgTime() {
		long avg = 0;
		for (Long value : timeProfile.values()) {
			avg += value;
		}
		avg = avg / timeProfile.size();
		return avg;
	}
	
	/**
	 * add a (key,value) pair to hash-table cache of the mapper task
	 * @param key
	 * @param value
	 */
	public void addCacheItem(String key, String value) {
		cacheList.add(new CacheItem(key, value));
	}
	
	public void addTimeCost(double startTime, double timeCost) {
		time_record.add(startTime);
		time_record.add(timeCost);
	}
	
	// Transmitting data via Socket
	public void sendData(DataOutputStream output) throws IOException{
		output.writeBytes(0 + "\n"); // Write data type first.
		output.writeBytes(getSize() + "\n");
		for (StatsType key : timeProfile.keySet()) {
			String content = "0" + ";" + key.toString() + ";" + timeProfile.get(key);
			//LOG.info("statistic content: " + content);
			output.writeBytes(content + "\n");
		}	
		for (CacheItem ci : cacheList)	{
			String content = "1" + ";" + ci.key + ";" + ci.value;
			//LOG.info("cache content: " + content);
			output.writeBytes(content + "\n");
		}
		if (time_record.size() == 2) {
			String content = "2" + ";" + time_record.get(0) + ";" + time_record.get(1);
			output.writeBytes(content + "\n");
		}
		//Log.warn("SendData done.");
	}

	//////////////////////////////////////////////
	// Writable
	//////////////////////////////////////////////
	public void write(DataOutput out) throws IOException {
		out.writeInt(getSize());
		for (StatsType key : timeProfile.keySet()) {
			Text.writeString(out, key.toString());
			out.writeLong(timeProfile.get(key));
		}		
	}		
	
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		for (int i=0; i<size; i++){
			String statType = Text.readString(in);
			long time = in.readLong();
			addTimeStat(new StatsType(statType), time);
		}
	}

	
}
