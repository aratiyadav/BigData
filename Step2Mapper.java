import java.io.IOException;
import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.json.JSONException;
import org.json.JSONObject;

public class Step2Mapper extends Mapper<Text, Text, Text, Text>
{
	public void map(Text key, Text values, Context context)
		throws IOException, InterruptedException
	{
		context.write(new Text(key), new Text(values));
	}
		
}
