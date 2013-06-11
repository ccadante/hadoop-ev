package myorg.offline;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class CarAggrReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>{
	private DoubleWritable result = new DoubleWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
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
		context.write(key, result, var);
	}
}
