import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UserAccessAnalyzer {
	//this mapper will treat one row from the input file
	//as the key, with a value of 1
	//
	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text row = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			
			StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
			while (itr.hasMoreTokens()) {
				row.set(itr.nextToken());
				context.write(row, one);
			}
		}
	}
	//custom partitioner class to partition on user id,
	//to identify which users access the server 
	//most often 
	public static class MonthPartitioner 
	extends Partitioner<Text,IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numReduceTasks) {
			String[] row = key.toString().split("\u0004");
			String accessDate = row[1];

			if(numReduceTasks == 0) {
				return 0;
			} 
			if( accessDate.substring(0,7).equals("2014-01") ){
				return 0; //pass to partition 0
			}
			else if( accessDate.substring(0,7).equals("2014-02") ){
				return 1 % numReduceTasks; //pass to partition 1
			}
			else if( accessDate.substring(0,7).equals("2014-03") ){
				return 2 % numReduceTasks; //pass to partition 2
			}
			else if( accessDate.substring(0,7).equals("2014-04") ){
				return 3 % numReduceTasks; //pass to partition 3
			}
			else if( accessDate.substring(0,7).equals("2014-05") ){
				return 4 % numReduceTasks; //pass to partition 4
			}
			else if( accessDate.substring(0,7).equals("2014-06") ){
				return 5 % numReduceTasks; //pass to partition 5
			}
			else if( accessDate.substring(0,7).equals("2014-07") ){
				return 6 % numReduceTasks; //pass to partition 6
			}
			else if( accessDate.substring(0,7).equals("2014-08") ){
				return 7 % numReduceTasks; //pass to partition 7
			}
			else if( accessDate.substring(0,7).equals("2014-09") ){
				return 8 % numReduceTasks; //pass to partition 8
			}
			else if( accessDate.substring(0,7).equals("2014-10") ){
				return 9 % numReduceTasks; //pass to partition 9
			}
			else if( accessDate.substring(0,7).equals("2014-11") ){
				return 10 % numReduceTasks; //pass to partition 10
			}
			else if( accessDate.substring(0,7).equals("2014-12") ){
				return 11 % numReduceTasks; //pass to partition 11
			} else {
				return 12 % numReduceTasks; //pass to partition 12 (default case)
			}
		}
	}  
	//reduce class definition will aggregate the number of access requests
	//per user during a given month
	//
	public static class IntSumReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}

			result.set(sum);
			context.write(key, result);
		}
	}
	//
	//main method
	//
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Server Access Analyzer");
		job.setNumReduceTasks(13); //will run with 13 reducers, one for each month and a default
		job.setJarByClass(MonthPartitioner.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setPartitionerClass(MonthPartitioner.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}