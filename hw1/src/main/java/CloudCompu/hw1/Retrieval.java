package CloudCompu.hw1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Retrieval {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String query ="Cat fly";
		conf.set("query", query);
		FileSystem fs = FileSystem.get(conf);
		// get the FileStatus list from given dir
		FileStatus[] status_list = fs.listStatus(new Path(args[1]));
		int N = status_list.length;
		conf.set("N", "" + N);

		conf.set("inputDir", args[1]);
		
		
		// Store global query for later usage
		Job job = Job.getInstance(conf, "Retrieval");
		job.setJarByClass(Retrieval.class);
		// Get input from key value pair
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		// set input format
		// setthe class of each stage in mapreduce
		job.setMapperClass(RetvalMapper.class);
		job.setPartitionerClass(RetvalPart.class);
		job.setReducerClass(RetvalReducer.class);
		job.setGroupingComparatorClass(RetvalGpCompare.class);
		job.setCombinerClass(RetvalCombi.class);

		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WordPos.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// set the number of reducer
		job.setNumReduceTasks(4);

		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
