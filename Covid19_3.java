
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Covid19_3 {                                                                                                                         
                                                                                           
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value                                        
	public static class MyMapper extends Mapper<Object, Text, Text, LongWritable> {                                                              
		private final static LongWritable one = new LongWritable(1);                                                                             
		private Text word = new Text();                                                                                                          

		// The 4 types declared here should match the types that was declared on the top                                                         
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {                                      

			if (value.toString().contains("new_cases"))                                                                                          
				return;                                                                                                                                       

			String dataValues[] = value.toString().split(",");                                                                                                                        
			Text val = new Text(dataValues[1]);                                                                                                  
			context.write(val, new LongWritable(Integer.parseInt(dataValues[2])));                                                                                                                                           
		}                                                                                                                                        
	}	                                                                                                                                         

	// 4 types declared: Type of input key, type of input value, type of output key, type of output value                                        
	// The input types of reduce should match the output type of map                                                                             
	public static class MyReducer extends Reducer<Text, LongWritable, Text, DoubleWritable> {                                                      
		private DoubleWritable total = new DoubleWritable();  
		private static Map<String, Long> populationHashMap = new HashMap<>();

		// Notice the that 2nd argument: type of the input value is an Iterable collection of objects                                            
		//  with the same type declared above/as the type of output value from map                                                               
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {                  
			long sum = 0;  
			double output = 0.0;
			for (LongWritable tmp: values) {                                                                                                     
				sum += tmp.get();                                                                                                                
			} 
			Long population = populationHashMap.get(key.toString());
			if(population == null) {
				System.out.println("No population for " + key);
				return;
			} 
			
			output = (double) sum / population.longValue() * 1000000;
			
			total.set(output);                                                                                                                      
			// This write to the final output                                                                                                    
			context.write(key, total);                                                                                                           
		}            

		public void setup(Context context){
			System.out.println("Coming here ___________");
			try {
				URI[] files = context.getCacheFiles();
				FileSystem hdfs = FileSystem.get(context.getConfiguration());
				System.out.println("Files are FFFFFFFFFFF "+files.length);
				System.out.println("Files are as ####### "+files[0]);
				for (URI file : files){
					try {
				        BufferedReader br = new BufferedReader(new InputStreamReader(
				                hdfs.open(new Path(file.getPath()))));
				        String data = br.readLine();

						while (data != null) {
							data = br.readLine();
                            System.out.println("File data is &&&&&& : " + data);
                            if(data == null || data.length() == 0)
                                continue;
							String dataValues[] = data.toString().split(",");
							if(dataValues.length < 5)
								continue;
							String country = dataValues[1];
							Long population = Long.parseLong(dataValues[4]);
							populationHashMap.put(country, population);
						}
						br.close();
					} catch (FileNotFoundException e ) {
						System.out.println("An error occurred.");
						e.printStackTrace();
					} catch (IOException e ) {
						System.out.println("An error occurred.");
						e.printStackTrace();
					}
				}
			} catch (IOException e) {
				System.out.println("IOException occurred.");
				e.printStackTrace();
			}

			for(Map.Entry<String, Long> entry : populationHashMap.entrySet()) {
				System.out.println("C "+ entry.getKey() + " "+ entry.getValue());
			}
		}
	}                                                                                                                                            


	public static void main(String[] args)  throws Exception {                                                                                   
		Configuration conf = new Configuration();                                                                                                          
		Job myjob = Job.getInstance(conf, "my word count test");                                                                                 
		myjob.setJarByClass(Covid19_3.class);                                                                                                    
		myjob.setMapperClass(MyMapper.class);                                                                                                    
		myjob.setReducerClass(MyReducer.class);                                                                                                  
		myjob.setOutputKeyClass(Text.class);                                                                                                     
		myjob.setOutputValueClass(LongWritable.class);  
		//		 Uncomment to set the number of reduce tasks                                                                                             
		//		 myjob.setNumReduceTasks(2);                                                                                                             
		FileInputFormat.addInputPath(myjob, new Path(args[0]));              
		//create FileSystem
		FileSystem fs = FileSystem.get(conf);
		myjob.addCacheFile(new Path("hdfs://" + fs.getCanonicalServiceName() + "/" + args[1]).toUri());                                                                     
		
		String output_Dir=args[2];
		Path outputDir=new Path(output_Dir);
		FileSystem hdfs = FileSystem.get(conf);
	    if (hdfs.exists(outputDir))
	      hdfs.delete(outputDir, true);

	    
	    
		FileOutputFormat.setOutputPath(myjob,  new Path(output_Dir));                                                                                
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);                                                                                      
	}                                                                                                                                            
}                                                                                                                                                