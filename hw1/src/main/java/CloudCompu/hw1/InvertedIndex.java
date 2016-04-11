package CloudCompu.hw1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "InvertedIndex");
		job.setJarByClass(InvertedIndex.class);
		
		FileSystem fs= FileSystem.get(conf); 
		//get the FileStatus list from given dir
		FileStatus[] status_list = fs.listStatus(new Path(args[0]));
		if(status_list != null){
		    for(FileStatus status : status_list){
		    	System.out.println(status.getPath().getName());
		    }
		}

		//set input format
		
		//job.setInputFormatClass(KeyValueTextInputFormat.class);
		/*
		 * Test String : 1,AG
		 * 				 3,BB
		 * TextInputFormat:
		 * 				(0,AG)
		 * 				(16,BB)
		 * KeyValueTextInputFormat:
		 * 				(1,AG)
		 * 				(3,BB)
		 * Can be config with mapreduce.input.keyvaluelinerecordreader.key.value.separato
		 */
		//setthe class of each stage in mapreduce
		job.setMapperClass(InvIdxExMapper.class);
		job.setPartitionerClass(InvIdxPart.class);
		job.setReducerClass(InvIdxExReducer.class);
		job.setCombinerClass(InvIdxExCombi.class);
		//job.setSortComparatorClass(InvIdxCompare.class);
		// job.setMapperClass(xxx.class);
		// job.setPartitionerClass(xxx.class);
		// job.setSortComparatorClass(xxx.class);
		// job.setReducerClass(xxx.class);

		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// job.setOutputKeyClass(xxx.class);
		// job.setOutputValueClass(xxx.class);

		// set the number of reducer
		job.setNumReduceTasks(2);

		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
