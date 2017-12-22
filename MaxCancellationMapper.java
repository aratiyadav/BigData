import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;
import java.io.*;


public class MaxCancellationMapper extends
    Mapper<LongWritable, Text, Text, Text> {
 
	
	public void map(LongWritable key, Text value, Context context)
	        throws IOException, InterruptedException{


		 
         String[] columnValues = value.toString().split(",");
         //StringTokenizer record = new StringTokenizer(line);
         
         String UniqueCarrier = columnValues[8];
         String origin = columnValues[16];
         String destination = columnValues[17];
         String cancel =columnValues[21];
         
         if(!columnValues[0].trim().toLowerCase().equals("year") && !origin.trim().toLowerCase().equals("na") && !destination.trim().toLowerCase().equals("na"))
         {
        	 
        	if(columnValues[21].trim().equals("1"))
        		context.write(new Text(UniqueCarrier.trim()), new Text(origin.trim()+ ","+ destination.trim()));
				
         }	
	}
}
			

	

	  
	  
	  
	  
	  
	  
	  
	    		
	    
      
            
       
 
      
   
  
    
  


    
