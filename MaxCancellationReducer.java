import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.io.*;

public class MaxCancellationReducer extends
    Reducer<Text, Text, Text, Text> {
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    
	  
	HashMap<String, Integer> maxMap = new HashMap<String, Integer>();
	
	
	
	int canValue = 0;
	
	
	for(Text val : values){
		canValue = 1;
		if(maxMap.containsKey(val.toString()))
		{
			canValue = maxMap.get(val.toString());
			canValue++;
		}
		maxMap.put(val.toString(), canValue);
		
	}
	
	Map.Entry<String, Integer> maxValue = null;
	for(Map.Entry<String, Integer> ent : maxMap.entrySet()){
		if(maxValue == null || ent.getValue().compareTo(maxValue.getValue()) > 0)
		{
			maxValue = ent;
		}
	}
		
	context.write(key, new Text(maxValue.getKey() + "\t" + maxValue.getValue()));
  }
  } 
  

