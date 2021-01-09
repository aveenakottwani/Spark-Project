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
 * 
 * 
 * 
 * To modify your program to report total number of deaths 
 * for every location/country in between a given range of dates.
 * @author AVEENA
 *
 */
public class Covid19_2 {
	
	public static final String SEPARATOR = ",";
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {
		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();
		
		
		// The 4 types declared here should match the types that was declared on the top
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			if (value.toString().contains("new_cases"))
                return;
			
			Configuration conf = context.getConfiguration();
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
			
			
			
			
			Date datavalue=null;
	        Date start_date = null; 
	        Date end_date=null;
	        
	        System.out.println("start_date _********** "+ start_date+"\n end_date "+end_date);

			String dataValues[] = value.toString().split(SEPARATOR);
			
			
			try {
				start_date = formatter.parse(conf.get("start_date"));
				end_date = formatter.parse(conf.get("end_date"));
				datavalue=formatter.parse(dataValues[0]);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        
	        
	        
			
			
			//Mapper code date check
			if((datavalue.compareTo(start_date) < 0 || datavalue.compareTo(end_date) > 0)) {
		    	System.out.println("Returning....Value is "+ start_date);
		    	return;
		    }

		    
			Text val = new Text(dataValues[1]);
			context.write(val, new LongWritable(Integer.parseInt(dataValues[3])));

		}
	}	

	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	// The input types of reduce should match the output type of map
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
	
	
	public static void main(String[] args)  throws Exception {
		Configuration conf = new Configuration();
		String worldFlag = "true";
		
		//arg[0]-> input file
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		
		
				
		
		// args[1] -> start date
		String start = args[1]; //"2020-04-09";
		//args[2] -> end-date
		String end = args[2]; //"2020-05-09";
		
		Date start_date =null;
		Date end_date =null;
		
		//args[3] -> output file
		
		
		//Condition 1:
		//Start date and end-date  before 1st Jan 2020 or after April 8th 2020- Invalid
				String start_real = "2020-01-01";
				String end_real = "2020-04-08";
				Date startreal =null;
				Date endreal =null;
				
		//Condition 2 : start and end are in correct format -> yyyy-MM-dd
		try {
			startreal=formatter.parse(start_real);
			endreal=formatter.parse(end_real);
			
			start_date = formatter.parse(start);
			// args[2] -> end-date
			end_date = formatter.parse(end);
			System.out.println("Valid format: start"+start_date + "  end" + (end_date));
			
		}
		/* Date format is invalid */
		catch (ParseException e) {
			System.out.println(start + " and " +end+" is Invalid Date format");
			System.out.println("Exiting code");
			// Terminate JVM
			System.exit(-1);
		}
		
		// Condition3 : start > end date
		if (start_date.compareTo(end_date) < 0) {
			System.out.println(" Start:" + start_date + " < end:" + end_date + " condn:"
					+ (start_date.compareTo(end_date) < 0));
		} 
		// Condition3 : start > end date
		else {
			System.out.println("False: Start:" + start_date + "> end:" + end_date + " condn:"
					+ (start_date.compareTo(end_date) < 0));
			System.out.println("Exiting code");
			// Terminate JVM
			System.exit(-1);
		}
		
		
		//Condition 1:
		//Start date and end-date  less than 1st Jan 2020 - Invalid
		if (start_date.compareTo(startreal) < 0  && end_date.compareTo(startreal) < 0)
		{
			System.out.println("Not valid dates. Both dates are before Jan 1 2020. Exiting code");
			// Terminate JVM
			System.exit(-1);
		}
		//start-date and End -date should be greater than April 8th 2020 -Invalid
		else if (start_date.compareTo(endreal) > 0  && end_date.compareTo(endreal) >0)
		{
			System.out.println("Not valid dates. Both dates are after Jan 1 2020. Exiting code");
			// Terminate JVM
			System.exit(-1);
		}
		
		
		
		conf.set("start_date",formatter.format(start_date));
		conf.set("end_date",formatter.format(end_date));
		Job myjob = Job.getInstance(conf, "my word count test");
		myjob.setJarByClass(Covid19_2.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
//		 Uncomment to set the number of reduce tasks
//		 myjob.setNumReduceTasks(2);
		
		String output_Dir=args[3];
		Path outputDir=new Path(output_Dir);
		FileSystem hdfs = FileSystem.get(conf);
	    if (hdfs.exists(outputDir))
	      hdfs.delete(outputDir, true);
	    
	    
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(output_Dir));
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}
}