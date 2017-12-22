	import java.io.IOException;
	import java.io.*;
	import java.util.HashMap;
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
	
	public class Step1Mapper extends Mapper<LongWritable, Text, Text, Text>
	{
		//HashMap Declaration
		HashMap<String, String> businessRatingDictionary = new HashMap<String, String>(); 
	
	
		//The setup method is implemented per split
	
		public void setup(Context context) throws IOException
		{
			Path pt = new Path("/hadoop-user/data/yelpJoinInput/yelp_academic_dataset_business.json");
			FileSystem fs = FileSystem.get(context.getConfiguration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
	
			String line = br.readLine();
	
			//Reading the JSON Business File and adding the values into HashMap businessRatingDictionary
	
			while(line != null)
			{
	
				JSONObject jsn1 = new JSONObject(line.toString());
	
				String business_id = (String)jsn1.get("business_id");
				String business_name = (String)jsn1.get("name");
	
				businessRatingDictionary.put(business_id, business_name );
	
				line = br.readLine();
			}
		}
	
	
		// Reading the JSON file for Business_id and User_id
	
		//The output of this map-side join would be key(business_id) and value(business_name + )
	
		public void map(LongWritable key, Text values, Context context)
				throws IOException, InterruptedException
		{
	
			//String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			//if (fileName.equals("yelp_academic_dataset_review.json"))
			//{
				try
				{
					JSONObject jsn2 = new JSONObject(values.toString());
	
					String business_id2 = (String)jsn2.get("business_id");
	
					String user_id = (String)jsn2.get("user_id");
	
	
					if(businessRatingDictionary.containsKey(business_id2))
					{
						String business_name = businessRatingDictionary.get(business_id2);
	
						//Emitting User_Id as key and Business_id, business_name values
	
						context.write(new Text(user_id), new Text(business_id2 + "\t" + business_name));
					}
				}
				catch(JSONException e)
				{
					e.printStackTrace();
				}
			//}
		}
	
	
	}
	
	
	
	
	
	
	
	
