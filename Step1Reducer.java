import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class Step1Reducer extends Reducer<Text, Text, Text, Text>
{
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException 
  {
	  //Adding all the values to ArrayList ARecords. ARecords contains concatenated values of Business ids and Business names.
	  List<String> ARecords = new ArrayList<String>(); 
	 
	  for(Text value:values)
	  {	
		ARecords.add(value.toString());
		
	  }
	
	  //Sorting the ArrayList
	  List<String> subList = ARecords.subList(1, ARecords.size());
	  Collections.sort(subList);
	
	  //Declaring variable to compare (Business_id + Business_name) values
	  String outValA_BusinessId = "";
	  String outValB_BusinessId = "";
	
	
	  for (int i = 0; i <subList.size(); i++)
	  {
		for(int j=1; j<subList.size();j++)
		{
			outValA_BusinessId = subList.get(i);
			outValB_BusinessId = subList.get(j);
			
			if(!outValA_BusinessId.equals(outValB_BusinessId))
			{
				context.write(new Text(outValA_BusinessId + "\t" + outValB_BusinessId), new Text(key));
			}
			
		}
		
	 }
  }
}

  
      
  


