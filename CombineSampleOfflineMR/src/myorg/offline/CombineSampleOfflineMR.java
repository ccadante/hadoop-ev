package myorg.offline;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
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

	/**
	 * @param args
	 * @throws IOException 
	 * @throws URISyntaxException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 * @throws CloneNotSupportedException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException, InstantiationException, IllegalAccessException, CloneNotSupportedException {
		// TODO Auto-generated method stub
//		System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.fileinputformat.split.maxsize", "67108864");
		DistributedCache.createSymlink(conf);
		DistributedCache.addCacheFile(new URI("hdfs://localhost:9000/libraries/libopencv_java244.so#libopencv_java244.so"), conf);
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: CombineOfflineMR <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "combile offline mapreduce");
		
		job.setJarByClass(CombineSampleOfflineMR.class);
		job.setMapperClass(ImageCarCountMapper.class);
		job.setReducerClass(CarAggrReducer.class);
		job.setInputFormatClass(CombineSampleInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//		InputFormat<?, ?> input = ReflectionUtils.newInstance(job.getInputFormatClass(), conf);
//		List<FileStatus> files = ((FileInputFormat)input).getListStatus(job);
/*
	    FileSystem fs = FileSystem.get(conf);
	    String samp_input_str = "";
		for(String file : inputs)
		{
			SequenceFile.Reader idxreader = new SequenceFile.Reader(fs, new Path(file, "index"), conf);
			/*
			String key = "cam1picfull_L.seq/1372953750366";
		    BytesWritab le value = new BytesWritable();
			mfreader.get(new Text(key), value);
			samp_input_str = samp_input_str + 
					file + ":" + key + ":" + ((BytesWritable)value).getLength() + ",";
			key = "cam1picfull_L.seq/1372953754727";
			mfreader.get(new Text(key), value);
			samp_input_str = samp_input_str + 
					file + ":" + key + ":" + ((BytesWritable)value).getLength() + ",";
			Log.info("$%%%%%%%%%%%%%% samp = " + samp_input_str);
			*/
/*			Text key = new Text();
		    LongWritable position = new LongWritable();
		    ArrayList<String> keylist = new ArrayList<String>();
		    ArrayList<Long> poslist = new ArrayList<Long>();
		    while(idxreader.next(key, position))
		    {
		    	keylist.add(key.toString());
		    	poslist.add(position.get());
		    }
		    
			String[] keyarr = keylist.toArray(new String[keylist.size()]);
			Long[] posarr = poslist.toArray(new Long[poslist.size()]);
			String[] inputarr = new String[keylist.size()];
			
			String k;
			Long pos1 = poslist.get(0);
			Long pos2;
			long length;
			for (int i=0; i<keyarr.length-1; i++)
			{
				k = keyarr[i];
				pos2 = posarr[i+1];
				length = pos2-pos1;
				pos1 = pos2;
				inputarr[i] = file + ":" + k + ":" + length;
//				samp_input_str = samp_input_str + file + ":" + k + ":" + length + ",";
				if(i%1000==0)
					Log.info("$%%%%%%%%%%%%%% key = " + k + "; length = " + length + "; count = " + i);
			}
			k = keyarr[keyarr.length-1];
			pos2 = fs.getFileStatus(new Path(file, "data")).getLen();
			length = pos2-pos1;
			inputarr[keyarr.length-1] = file + ":" + k + ":" + length;
//			samp_input_str = samp_input_str + file + ":" + k + ":" + length + ",";
			Log.info("$%%%%%%%%%%%%%% key = " + k + "; length = " + length + "; count = " + keylist.size());
			
			samp_input_str = StringUtils.join(inputarr, ",");
		}
		
		job.getConfiguration().set(SampleInputUtil.SAMP_DIR, samp_input_str);
		*/
//		System.exit(job.waitForCompletion(true) ? 0 : 1);
		System.exit(job.waitForSampleCompletion() ? 0 : 1);

	}
	
	
	public static class ImageCarCountMapper extends Mapper<Text, BytesWritable, Text, IntWritable>{
		private CascadeClassifier car_cascade;
		private String cas_file = "cars3.xml";
		
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException
		{
			System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
			System.out.println("host: " + InetAddress.getLocalHost().getHostName());
			long startTime = System.currentTimeMillis();
			int VC = CountVehicle(value);
			long usedTime = System.currentTimeMillis() -startTime;
			String folder = key.toString();
			folder = folder.substring(0, folder.lastIndexOf("/"));
			System.out.println("key: " + key);
			System.out.println("folder: " + folder);
			System.out.println("number: " + VC);
			System.out.println("total: " + usedTime);
			context.write(new Text(folder), new IntWritable(VC));
		}
		
		public int CountVehicle(BytesWritable value) throws IOException
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
			System.out.println("vec2mat: " + (System.currentTimeMillis()-s));
			
			s = System.currentTimeMillis();
			img = Highgui.imdecode(img, Highgui.CV_LOAD_IMAGE_COLOR);
			System.out.println("imdec: " + (System.currentTimeMillis()-s));

			int vehicleCount = 0;
			int frameCount = 0;
			Mat frame_gray = new Mat();
			
			if (!img.empty())
			{
//				System.out.println("*** frame - " + (++frameCount) + " ***  " + car_cascade.empty());
			    	
				MatOfRect cars = new MatOfRect();
				s = System.currentTimeMillis();
				Imgproc.cvtColor( img, frame_gray, Imgproc.COLOR_BGR2GRAY );
				System.out.println("cvtclr: " + (System.currentTimeMillis()-s));

				s = System.currentTimeMillis();
				Imgproc.equalizeHist( frame_gray, frame_gray );
				System.out.println("eqlhist: " + (System.currentTimeMillis()-s));

				s = System.currentTimeMillis();
				car_cascade.detectMultiScale( frame_gray, cars, 1.1, 2, 0, new Size(40, 40), new Size(800, 800) );
				System.out.println("detect: " + (System.currentTimeMillis()-s));
				vehicleCount = vehicleCount + cars.height();
				
			}
			return vehicleCount;
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
			for(IntWritable val:values)
			{
				sum += val.get();
				valList.add(val.get());
				count++;
			}
			avg = sum / (double) count;
			for(Integer val: valList)
			{
				var += Math.pow(val - avg, 2);
			}
			var = var / (double) count;
			//result.set(sum);
			result.set(avg);
			System.out.println("mean=" + result.toString() + " variance=" + var);
			context.write(key, result, var);
		}
	}
}

