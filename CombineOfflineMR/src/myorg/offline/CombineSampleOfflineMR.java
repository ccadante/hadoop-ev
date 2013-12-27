package myorg.offline;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineSampleInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SampleInputUtil;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
//import org.mortbay.log.Log;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.MatOfRect;
import org.opencv.core.Size;
import org.opencv.highgui.Highgui;
import org.opencv.imgproc.Imgproc;
import org.opencv.objdetect.CascadeClassifier;
import org.opencv.utils.Converters;

public class CombineSampleOfflineMR {
	final static long SIZE_1M = 1024 * 1024;
	final static String whiteListFile = "/home/temp/Projects/hadoop-ev/conf/dataWhiteList";
	// 425 - 25G, time
	// 501 - loadbalance
	final static int[] datasizeList = {425, 450, 475, 4100};
	final static int[] deadlineList = {20, 25, 30, 35, 40, 45, 50, 55, 60};
	final static int[] deadlineList1 = {45, 50, 55, 60};
	//final static int[] deadlineList = {45, 55};
	// 00: MaReV + loadblance    01: w/o loadbalance
	// 10: random + loadbalance    11: random w/o loadbalance
	//final static int[] policyList = {00, 01, 10, 11};
	final static int[] policyList = {00, 10, 11};
	final static int[] policyList2 = {10, 11};
	//final static int[] loadbalanceSizeList = {25, 50, 75, 100};
	
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException, InstantiationException, IllegalAccessException, CloneNotSupportedException {
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.fileinputformat.split.maxsize", "33558864");
		conf.set("mapred.sample.experimentCount", "30");
		
		conf.set("mapred.sample.policy", "0");
		conf.set("mapred.deadline.second", "90");
		conf.set("mapred.sample.sizePerFolder", "7");
		conf.set("mapred.sample.sampleTimePctg", "0.2");
		conf.set("mapred.sample.splitsCoeff", "1.5");
		conf.set("mapred.sample.splitsTimeCoeff", "1.0");
		conf.set("mapred.filter.startTimeOfDay", "7");
		conf.set("mapred.filter.endTimeOfDay", "20");
		// 0 - not test (normal execution); 1 - splitByTime; 2 - splitBySize
		conf.set("mapred.sample.testLoadBalance", "0");
		conf.set("mapred.sample.testLoadBalanceSize", String.valueOf(1 * SIZE_1M));
		
		conf.set("mapred.sample.enableWhiteList", "false");		
		conf.set("mapred.sample.whiteList", "");	
		
		DistributedCache.createSymlink(conf);
		DistributedCache.addCacheFile(new URI(conf.get("fs.default.name") + "/libraries/libopencv_java244.so#libopencv_java244.so"), conf);
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: CombineSampleOfflineMR <in> <out>");
			System.exit(2);
		}
		
		conf.set("mapred.sample.enableWhiteList", "true");
		ArrayList<String> whiteListStr = getWhiteList();	
		conf.set("mapred.sample.whiteList", whiteListStr.get(0));			
		
		Job.resetInputDataSet();
		conf.set("mapred.sample.testLoadBalance", "0");
		for (int deadline: deadlineList) {
			for (int policy:  policyList2) {
				conf.set("mapred.sample.policy", String.valueOf(policy));
				conf.set("mapred.deadline.second", String.valueOf(deadline));
				
				String suffix = "-" + deadline + "s-" + policy;
				
				// Reset StatsServer first! 
				Job.resetStatsServer();
				Job oneJob = new Job(conf, "vehicleCounting" + suffix);
				
				oneJob.setJarByClass(CombineSampleOfflineMR.class);
				oneJob.setMapperClass(ImageCarCountMapper.class);
				oneJob.setReducerClass(CarAggrReducer.class);
				oneJob.setInputFormatClass(CombineSampleInputFormat.class);
				oneJob.setOutputKeyClass(Text.class);
				oneJob.setOutputValueClass(IntWritable.class);
				
				FileInputFormat.setInputPaths(oneJob, new Path(otherArgs[0]));
				FileOutputFormat.setOutputPath(oneJob, new Path(otherArgs[1] + suffix));
				oneJob.waitForSampleCompletion();
				
				Thread.sleep(1000);
			}			
		}
		
		conf.set("mapred.deadline.second", "45");
		conf.set("mapred.sample.enableWhiteList", "true");
		conf.set("mapred.sample.testLoadBalance", "0");
		for (int i = 1; i< whiteListStr.size(); i++) {
			Job.resetInputDataSet();
			for (int policy:  policyList) {				
				conf.set("mapred.sample.whiteList", whiteListStr.get(i));
				conf.set("mapred.sample.policy", String.valueOf(policy));
				
				String suffix = "-" + datasizeList[i] + "G-" + policy;
				
				// Reset StatsServer first! 
				Job.resetStatsServer();
				Job oneJob = new Job(conf, "vehicleCounting" + suffix);
				
				oneJob.setJarByClass(CombineSampleOfflineMR.class);
				oneJob.setMapperClass(ImageCarCountMapper.class);
				oneJob.setReducerClass(CarAggrReducer.class);
				oneJob.setInputFormatClass(CombineSampleInputFormat.class);
				oneJob.setOutputKeyClass(Text.class);
				oneJob.setOutputValueClass(IntWritable.class);
				
				FileInputFormat.setInputPaths(oneJob, new Path(otherArgs[0]));
				FileOutputFormat.setOutputPath(oneJob, new Path(otherArgs[1] + suffix));
				oneJob.waitForSampleCompletion();
				
				Thread.sleep(1000);
			}			
		}
		
		/*Job.resetInputDataSet();
		conf.set("mapred.sample.sizePerFolder", "50");
		conf.set("mapred.sample.sampleTimePctg", "0.8");
		for (int size : loadbalanceSizeList) {
			for (int test = 1; test <= 2; test++) {
				if (test == 1) { // split by time
					for (int coeff = 1; coeff <= 2; coeff++) {
						conf.set("mapred.sample.splitsTimeCoeff", String.valueOf(coeff));
						conf.set("mapred.sample.testLoadBalance", String.valueOf(test));
						conf.set("mapred.sample.testLoadBalanceSize", String.valueOf(size * SIZE_1M));
						
						String suffix = "-" + size + "M-" + test;
						
						// Reset StatsServer first! 
						Job.resetStatsServer();
						Job oneJob = new Job(conf, "vehicleCounting" + suffix);
						
						oneJob.setJarByClass(CombineSampleOfflineMR.class);
						oneJob.setMapperClass(ImageCarCountMapper.class);
						oneJob.setReducerClass(CarAggrReducer.class);
						oneJob.setInputFormatClass(CombineSampleInputFormat.class);
						oneJob.setOutputKeyClass(Text.class);
						oneJob.setOutputValueClass(IntWritable.class);
						
						FileInputFormat.setInputPaths(oneJob, new Path(otherArgs[0]));
						FileOutputFormat.setOutputPath(oneJob, new Path(otherArgs[1] + suffix));
						oneJob.waitForSampleCompletion();
						
						Thread.sleep(1000);
					}
				} else if (test == 2) { // split by size
					for (int coeff = 1; coeff <= 2; coeff++) {
						conf.set("mapred.sample.splitsCoeff", String.valueOf(coeff));
						conf.set("mapred.sample.testLoadBalance", String.valueOf(test));
						conf.set("mapred.sample.testLoadBalanceSize", String.valueOf(size * SIZE_1M));
						
						String suffix = "-" + size + "M-" + test;
						
						// Reset StatsServer first! 
						Job.resetStatsServer();
						Job oneJob = new Job(conf, "vehicleCounting" + suffix);
						
						oneJob.setJarByClass(CombineSampleOfflineMR.class);
						oneJob.setMapperClass(ImageCarCountMapper.class);
						oneJob.setReducerClass(CarAggrReducer.class);
						oneJob.setInputFormatClass(CombineSampleInputFormat.class);
						oneJob.setOutputKeyClass(Text.class);
						oneJob.setOutputValueClass(IntWritable.class);
						
						FileInputFormat.setInputPaths(oneJob, new Path(otherArgs[0]));
						FileOutputFormat.setOutputPath(oneJob, new Path(otherArgs[1] + suffix));
						oneJob.waitForSampleCompletion();
						
						Thread.sleep(1000);
					}
				}
			}			
		}*/
		
		/*Job job = new Job(conf, "vehicleCounting");		
		job.setJarByClass(CombineSampleOfflineMR.class);
		job.setMapperClass(ImageCarCountMapper.class);
		job.setReducerClass(CarAggrReducer.class);
		job.setInputFormatClass(CombineSampleInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForSampleCompletion() ? 0 : 1);*/
		
		System.exit(0);
	}
	
	public static ArrayList<String> getWhiteList() {
		ArrayList<String> ret = new ArrayList<String>();
		try {
  		  BufferedReader br = new BufferedReader(new FileReader(whiteListFile));
  		  String line;
  		  while ((line = br.readLine()) != null) {
  			  if (line.length() < 10 )
  				  continue;
  			  String sizeStr = line.substring(0, line.indexOf(","));
  			  int size = Integer.parseInt(sizeStr);
  			  if(ret.size() < datasizeList.length && size == datasizeList[ret.size()]) {
  				  ret.add(line);
  				  if (ret.size() == datasizeList.length) 
  					  break;
  			  }
  		  }
  		  br.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return ret;
	}
	
	public static class ImageCarCountMapper extends Mapper<Text, BytesWritable, Text, IntWritable>{
		private CascadeClassifier car_cascade;
		private String cas_file = "cars3.xml";
		
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException
		{
			System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
			System.out.println("host: " + InetAddress.getLocalHost().getHostName());
			long startTime = System.currentTimeMillis();
			int VC = CountVehicle(value, key.toString());
			long usedTime = System.currentTimeMillis() -startTime;
			String folder = key.toString();
			folder = folder.substring(0, folder.lastIndexOf("/"));			
			context.write(new Text(folder), new IntWritable(VC));
			System.out.println(key + " " + usedTime + " " + VC);
		}
		
		public int CountVehicle(BytesWritable value, String key) throws IOException
		{
			car_cascade = new CascadeClassifier();
			car_cascade.load(cas_file);

			byte[] filecontent = value.getBytes();
			Mat img = new Mat();
			Byte[] BigByteArray = new Byte[filecontent.length];
			for (int i=0; i<filecontent.length; i++)
			{
				BigByteArray[i] = new Byte(filecontent[i]);
			}
			System.out.println("size: " + filecontent.length);
			List<Byte> matlist = Arrays.asList(BigByteArray);
			long s = System.currentTimeMillis();
			img = Converters.vector_char_to_Mat(matlist);
			//System.out.println("vec2mat: " + (System.currentTimeMillis()-s));
			
			s = System.currentTimeMillis();
			img = Highgui.imdecode(img, Highgui.CV_LOAD_IMAGE_COLOR);
			//System.out.println("imdec: " + (System.currentTimeMillis()-s));

			int vehicleCount = -1;
			Mat frame_gray = new Mat();
			
			if (!img.empty())
			{
				MatOfRect cars = new MatOfRect();
				s = System.currentTimeMillis();
				Imgproc.cvtColor( img, frame_gray, Imgproc.COLOR_BGR2GRAY );
				//System.out.println("cvtclr: " + (System.currentTimeMillis()-s));
				
				if (!isCorruptImg(frame_gray)) {
					s = System.currentTimeMillis();
					Imgproc.equalizeHist( frame_gray, frame_gray );
					//System.out.println("eqlhist: " + (System.currentTimeMillis()-s));
	
					s = System.currentTimeMillis();
					car_cascade.detectMultiScale( frame_gray, cars, 1.10, 2, 0, new Size(10, 10), new Size(800, 800) );
					//System.out.println("detect: " + (System.currentTimeMillis()-s));
					vehicleCount = vehicleCount + cars.height();
					if ((System.currentTimeMillis()-s) > 1000 || (System.currentTimeMillis()-s) < 100) {			
						String dump = frame_gray.dump();
						System.out.println(dump);
					}
				}				
			}
			return vehicleCount;
	    }
		
		private boolean isCorruptImg(Mat grayImg) {
			return (Core.norm(grayImg.row(grayImg.rows() - 1), grayImg.row(grayImg.rows() - 5)) == 0)
					|| (Core.norm(grayImg.row(grayImg.rows() - 3), grayImg.row(grayImg.rows() - 7)) == 0)
					|| (Core.norm(grayImg.row(grayImg.rows() - 50), grayImg.row(grayImg.rows() - 55)) == 0); 
		}
	}
	
	public static class CarAggrReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>{
		private DoubleWritable result = new DoubleWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			/*int sum = 0;
			for(IntWritable val:values)
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);*/
			
			int sum = 0;
			int count = 0;
			double var = 0.0;
			double avg = 0.0;
			ArrayList<Integer> valList = new ArrayList<Integer>();
			String valStr = "";
			for(IntWritable val:values)
			{
				if (val.get() < 0)
					continue;
				sum += val.get();
				valList.add(val.get());
				valStr += val.get() + " ";
				count++;
			}
			if (count > 0)
				avg = sum / (double) count;
			for(Integer val: valList)
			{
				var += Math.pow(val - avg, 2);
			}
			if (count > 0) {
				var = var / (double) count; // variance of original variable, x
				var = var / (double) count; // variance of mean(x) by count observations
			}
			//result.set(sum);
			result.set(avg);
			System.out.println(key +" avg = " + result.toString() + "  var = " + var
					+ "  count = " + count);
			System.out.println(valStr);
			
			context.write(key, result, var, count);
		}
	}
}

