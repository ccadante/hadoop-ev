package myorg.offline;

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
//		System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
		
		Configuration conf = new Configuration();
		conf.set("mapreduce.input.fileinputformat.split.maxsize", "33558864");
		DistributedCache.createSymlink(conf);
		DistributedCache.addCacheFile(new URI(conf.get("fs.default.name") + "/libraries/libopencv_java244.so#libopencv_java244.so"), conf);
		
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
					car_cascade.detectMultiScale( frame_gray, cars, 1.1, 2, 0, new Size(10, 10), new Size(800, 800) );
					//System.out.println("detect: " + (System.currentTimeMillis()-s));
					vehicleCount = vehicleCount + cars.height();
					if ((System.currentTimeMillis()-s) > 700 || (System.currentTimeMillis()-s) < 100) {			
						String dump = frame_gray.dump();
						System.out.println(dump);
					}
				}				
			}
			return vehicleCount;
	    }
		
		private boolean isCorruptImg(Mat grayImg) {
			return ((Core.norm(grayImg.row(grayImg.rows() - 1), grayImg.row(grayImg.rows() - 5)) == 0)
					&& (Core.norm(grayImg.row(grayImg.rows() - 7), grayImg.row(grayImg.rows() - 11)) == 0))
					|| ((Core.norm(grayImg.row(grayImg.rows() - 50), grayImg.row(grayImg.rows() - 55)) == 0)
						&& (Core.norm(grayImg.row(grayImg.rows() - 57), grayImg.row(grayImg.rows() - 61)) == 0))
					|| ((Core.norm(grayImg.row(grayImg.rows() - 100), grayImg.row(grayImg.rows() - 105)) == 0)
						&& (Core.norm(grayImg.row(grayImg.rows() - 107), grayImg.row(grayImg.rows() - 111)) == 0)); 
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
				if (val.get() < 0)
					continue;
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
			System.out.println(key +" avg = " + result.toString() + "  var = " + var
					+ "  count = " + count);
			
			context.write(key, result, var, count);
		}
	}
}

