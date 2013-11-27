package myorg.offline;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

import javax.imageio.ImageIO;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.MatOfRect;
import org.opencv.core.Point;
import org.opencv.core.Rect;
import org.opencv.core.Scalar;
import org.opencv.core.Size;
import org.opencv.highgui.Highgui;
import org.opencv.imgproc.Imgproc;
import org.opencv.objdetect.CascadeClassifier;
import org.opencv.utils.Converters;
	
public class ImageCarCountMapper extends Mapper<Text, BytesWritable, Text, IntWritable>{
	private CascadeClassifier car_cascade;
	private String cas_file = "cars3.xml";
	
	public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException
	{/*
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		String imageDir = fileSplit.getPath().toString();
		System.out.println("<MyLog>: " + imageDir);
		int VC = 0;
		try {
			String[] dirsec = imageDir.split("/");
			String localDir = "./imginput/" + dirsec[dirsec.length-2] + "/" + dirsec[dirsec.length-1];
			String command = "/home/fan/Dropbox/EVCloud/Project/myVC/build3/myVC_img " + localDir;
			Process p=Runtime.getRuntime().exec(command); 
			p.waitFor(); 
			BufferedReader reader=new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line=reader.readLine(); 
			while(line!=null) {
				System.out.println(command);
				System.out.println(line);
				if(line.contains("FINAL")){
					String[] str = line.split(" ");
					if(str.length == 2){
						VC = Integer.parseInt(str[1]);
					}
				}
				line=reader.readLine(); 
			}
		} 
		catch(IOException e1) {} 
		catch(InterruptedException e2) {} 
		*/
		System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
		//System.out.println("host: " + InetAddress.getLocalHost().getHostName());
		long startTime = System.currentTimeMillis();
		int VC = CountVehicle(value);
		long usedTime = System.currentTimeMillis() -startTime;
		//System.out.println("number: " + VC);
		//System.out.println("total: " + usedTime);
		String folder = key.toString();
		folder = folder.substring(0, folder.lastIndexOf("/"));
		context.write(new Text(folder), new IntWritable(VC));
		System.out.println(key + " " + usedTime + " " + VC);
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
		//System.out.println("size: " + filecontent.length);
		List<Byte> matlist = Arrays.asList(BigByteArray);
		long s = System.currentTimeMillis();
		img = Converters.vector_char_to_Mat(matlist);
		//System.out.println("vec2mat: " + (System.currentTimeMillis()-s));
		
		s = System.currentTimeMillis();
		img = Highgui.imdecode(img, Highgui.CV_LOAD_IMAGE_COLOR);
		//System.out.println("imdec: " + (System.currentTimeMillis()-s));

		int vehicleCount = 0;
		int frameCount = 0;
		Mat frame_gray = new Mat();
		
		if (!img.empty())
		{
//			System.out.println("*** frame - " + (++frameCount) + " ***  " + car_cascade.empty());
		    	
			MatOfRect cars = new MatOfRect();
			s = System.currentTimeMillis();
			Imgproc.cvtColor( img, frame_gray, Imgproc.COLOR_BGR2GRAY );
			//System.out.println("cvtclr: " + (System.currentTimeMillis()-s));

			s = System.currentTimeMillis();
			Imgproc.equalizeHist( frame_gray, frame_gray );
			//System.out.println("eqlhist: " + (System.currentTimeMillis()-s));

			s = System.currentTimeMillis();
			car_cascade.detectMultiScale( frame_gray, cars, 1.1, 2, 0, new Size(40, 40), new Size(800, 800) );
			//System.out.println("detect: " + (System.currentTimeMillis()-s));
			vehicleCount = vehicleCount + cars.height();
			
//			Rect[] car_arr = cars.toArray();
//			for( int i = 0; i < car_arr.length; i++ )
//			{
//				System.out.println(car_arr[i].x + ", " + car_arr[i].y + ", " + car_arr[i].width + ", " +  car_arr[i].height);
//				Core.rectangle(img, new Point(car_arr[i].x, car_arr[i].y), new Point(car_arr[i].x+car_arr[i].width, car_arr[i].y+car_arr[i].height), new Scalar(0,0,255), 2);
//			}
//			Highgui.imwrite("result.jpg", img);
		}
		return vehicleCount;
    }
}