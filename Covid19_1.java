import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * Count the 
 * 
 * total number of reported cases for 
 * 
 * every country/location till April 8th, 2020
 * 
 * 
 * @author AVEENA
 *
 */
public class Covid19_1 {
	
	public static final String SEPARATOR = ",";
	/* 4 types declared: Type of input key, type of input value, type of output key, type of output value
	 * 
	 * 
	 * 
	 */
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();
		

		
		/* The 4 types declared here should match the types that was declared on the top
		 *
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
		 * 
		 * @author Aveena Kottwani
		 * @return 
		 */
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			if (value.toString().contains("new_cases"))
                return;
			
			Configuration conf = context.getConfiguration();
	        String worldFlag = conf.get("worldFlag");
	        System.out.println("World flag _********** "+ worldFlag);

			String dataValues[] = value.toString().split(SEPARATOR);
	        if(worldFlag.equalsIgnoreCase("false") && dataValues[1].equalsIgnoreCase("world"))
	        	return;
	        
	        String start="2020-01-01",end="2020-04-08";
	       
	        Date start_date=null,endDate=null,dataValue=null;
	        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		    try {
		    	start_date = dateFormat.parse(start);
			    
		    	endDate = dateFormat.parse(end);
		    	dataValue=dateFormat.parse(dataValues[0]);
			    
		    } catch(ParseException e) {
		    	System.out.println("Exception "+ e);
		    }
	        
	        
	        /**
	         * Comparison of dates should be between 1st Jan 2020 to 8th April 2020
	         */
		    if( (dataValue.compareTo(start_date) < 0 ) || (dataValue.compareTo(endDate) > 0)) {
		    	System.out.println("Value is "+ dataValues[0]);
		    	return;
		    }
			Text val = new Text(dataValues[1]);
			context.write(val, new LongWritable(Integer.parseInt(dataValues[2])));

		}
	}	

	/* 4 types declared: Type of input key, type of input value, type of output key, type of output value
	 *
	 * The input types of reduce should match the output type of map 
	 * 
	 */
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable total = new LongWritable();
		
		// Notice the that 2nd argument: type of the input value is an Iterable collection of objects 
		//  with the same type declared above/as the type of output value from map
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable tmp: values) {
				sum += tmp.get();
			}
			total.set(sum);
			// This write to the final output
			context.write(key, total);
		}
	}
	
	/*
	 * Main class to  count the 
	 * 
	 * total number of reported cases for 
	 * 
	 * every country/location till April 8th, 2020
	 * 
	 * 
	 */
	public static void main(String[] args)  throws Exception {
		long startTime = System.nanoTime();

		Configuration conf = new Configuration();
		String worldFlag = "true",output_Dir=null, inputDir=null ;
		
		/*
		 * Program arguments description
		 * The HDFS path to your input data file
		 * [true | false] Include "World" data in the result or not.
		 *  True will include total number of reported cases for "World" in the result, 
		 *  False will ignore the rows with location/country = world
		 * The HDFS output path for your program. 
		 */
		
		System.out.println(args[0] +" "+args[2] +" "+args[2] +" "+args.length);

		if(args != null && args.length > 2)
			{
				inputDir = args[0];
				worldFlag = args[1];
				output_Dir=args[2];
			}
		else if(args != null && args.length == 2)
		{
			inputDir = args[0];
			output_Dir=args[1];
		}
		
		conf.set("worldFlag",worldFlag);
		Job myjob = Job.getInstance(conf, "Covid19_1 test");
		myjob.setJarByClass(Covid19_1.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
//		 Uncomment to set the number of reduce tasks
		 myjob.setNumReduceTasks(1);
		
		
		Path outputDir=new Path(output_Dir);
		FileSystem hdfs = FileSystem.get(conf);
	    if (hdfs.exists(outputDir))
	      hdfs.delete(outputDir, true);
	    
	    
		FileInputFormat.addInputPath(myjob, new Path(inputDir));
		FileOutputFormat.setOutputPath(myjob,  new Path(output_Dir));
		Boolean out=myjob.waitForCompletion(true);
		System.out.println(out );
		long endTime   = System.nanoTime();
		long totalTime = (endTime - startTime)/1000000;
		System.out.println("Total-time:"+totalTime);
		System.exit(out ? 0 : 1);
		
	}
}