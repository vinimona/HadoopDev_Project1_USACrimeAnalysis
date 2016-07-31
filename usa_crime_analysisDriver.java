package USA_CRIME_ANALYSIS;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class usa_crime_analysisDriver {

	public static void main (String args[]) throws IOException, InterruptedException, ClassNotFoundException
	{
		// Job for submitting mapper/reducer code for counting cases under each FBI code
		@SuppressWarnings("deprecation")
		Job job1 = new Job();
		job1.setJar("/home/acadgild/workspace/viniHadoop/src/usa_crime_analysis.jar");
			
		FileInputFormat.addInputPath(job1, new Path(args[0]));   // Input file path
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));  // Output file path
			
		job1.setMapperClass(casesUnderEachFBICodeMap.class);
		job1.setCombinerClass(usaCrimeAnalysisReduce.class);
		job1.setReducerClass(usaCrimeAnalysisReduce.class);
			
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
			
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
			
		//Start Execution by submitting the job
		@SuppressWarnings("unused")
		boolean job1_status = job1.waitForCompletion(true);
		//System.exit( job1_status ? 0 : 1);
		
		// Job for submitting mapper/reducer code for counting cases under FBI code 32
		@SuppressWarnings("deprecation")
		Job job2 = new Job();
		job2.setJar("/home/acadgild/workspace/viniHadoop/src/usa_crime_analysis.jar");
			
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
			
		job2.setMapperClass(casesUnderFBICode32Map.class);
		job2.setCombinerClass(usaCrimeAnalysisReduce.class);
		job2.setReducerClass(usaCrimeAnalysisReduce.class);
			
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);
			
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
			
		//Start Execution by submitting the job
		@SuppressWarnings("unused")
		boolean job2_status = job2.waitForCompletion(true);
		//System.exit( job2_status ? 0 : 1);
		
		// Job for submitting mapper/reducer code for counting number of arrests in theft district wise
		@SuppressWarnings("deprecation")
		Job job3 = new Job();
		job3.setJar("/home/acadgild/workspace/viniHadoop/src/usa_crime_analysis.jar");
					
		FileInputFormat.addInputPath(job3, new Path(args[0]));
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));
					
		job3.setMapperClass(arrestsForTheftUnderEachDistrictMap.class);
		job3.setCombinerClass(usaCrimeAnalysisReduce.class);
		job3.setReducerClass(usaCrimeAnalysisReduce.class);
					
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(IntWritable.class);
					
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);
					
		//Start Execution by submitting the job		
		@SuppressWarnings("unused")
		boolean job3_status = job3.waitForCompletion(true);
		//System.exit( job3_status ? 0 : 1);		
		
		// Job for submitting mapper/reducer code for counting number of arrests done between October 2014 to October 2015
		@SuppressWarnings("deprecation")
		Job job4 = new Job();
		job4.setJar("/home/acadgild/workspace/viniHadoop/src/usa_crime_analysis.jar");
							
		FileInputFormat.addInputPath(job4, new Path(args[0]));
		FileOutputFormat.setOutputPath(job4, new Path(args[4]));
					
		job4.setMapperClass(arrestsBetweenOct2014To2015Map.class);
		job4.setCombinerClass(usaCrimeAnalysisReduce.class);
		job4.setReducerClass(usaCrimeAnalysisReduce.class);
							
		job4.setMapOutputKeyClass(Text.class);
		job4.setMapOutputValueClass(IntWritable.class);
					
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(IntWritable.class);
						
		//Start Execution by submitting the job	
		boolean job4_status = job4.waitForCompletion(true);
		System.exit( job4_status ? 0 : 1);		
					
	}
}


