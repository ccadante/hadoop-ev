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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.mortbay.log.Log;

public class EVStatistics {
	public Map<StatsType, Long> timeProfile = new HashMap<StatsType, Long>();
	public Map<String, String> aggreStats = new HashMap<String, String>();
	
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
			total += value / 1000.0;
		}
		
		public void addDiff(long value) {
			varTotal += Math.pow((value / 1000.0 - avg), 2);
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
	
	public EVStatistics(){
		timeProfile.clear();
	}
	
	public void clear() {
		timeProfile.clear();
		aggreStats.clear();
	}

	public void addTimeStat(StatsType type, long time) {
		timeProfile.put(type, time);
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
	
	// Transmitting data via Socket
	public void sendData(DataOutputStream output) throws IOException{
		output.writeBytes(getSize() + "\n");
		for (StatsType key : timeProfile.keySet()) {
			String content = key.toString() + ";" + timeProfile.get(key);
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
