package CloudCompu.hw1;

import org.apache.hadoop.conf.Configuration;
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
		conf.set("Query", "AJAX");
		//Store global query for later usage
		Job job = Job.getInstance(conf, "Retrieval");
		job.setJarByClass(Retrieval.class);
		//Get input from key value pair
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		// set input format
		// setthe class of each stage in mapreduce
		job.setMapperClass(InvIdxExMapper.class);
		job.setPartitionerClass(InvIdxPart.class);
		job.setReducerClass(InvIdxExReducer.class);
		job.setCombinerClass(InvIdxExCombi.class);

		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// set the number of reducer
		job.setNumReduceTasks(2);

		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
