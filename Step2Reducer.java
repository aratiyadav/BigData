import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import java.util.*;


public class Step2Reducer extends Reducer<Text, Text, Text, IntWritable>
{
	public void reduce(Text key, Iterable<Text>values, Context context)
		throws IOException, InterruptedException
	{
		int count = 0;
		for(Text value: values)
		{
			count += 1;
			
		}
		context.write(new Text(key), new IntWritable(count));
	}
}
