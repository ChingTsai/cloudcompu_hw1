package CloudCompu.hw1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class RetvalMapper extends Mapper<Text, Text, Text, MapWritable>{
	public void map(Text key, Text value, Context context)
			throws IOException, InterruptedException {

		//Replace nonAlphabetic with space
		StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("[^a-zA-Z]", " "));
		
		while (itr.hasMoreTokens()) {
			String toProcess = itr.nextToken();
			
				word.set(toProcess);
				map.put(new Text(filename), new IntWritable(1));
				context.write(word, map);
			
		}

	}
}
