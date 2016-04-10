package CloudCompu.hw1;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RetvalMapper extends Mapper<Text, Text, Text, MapWritable> {
	private MapWritable map = new MapWritable();
	private Text word = new Text();

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		// Replace nonAlphabetic with space
		StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("[^a-zA-Z]", " "));
		int df, tf;
		
		LinkedList<DoubleWritable> l = new LinkedList<DoubleWritable>();
		String fileName;
			df = Integer.parseInt(itr.nextToken());
		for (int i = 0; i < df; i++) {
			fileName = itr.nextToken();
			tf = Integer.parseInt(itr.nextToken());
			l.clear();
			for (int j = 0; j < tf; j++) {
				l.add(new DoubleWritable(Long.parseLong(itr.nextToken())));
			}
			l.add(new DoubleWritable());
			map.put(new Text(fileName), value);
		}

		/*
		 * while (itr.hasMoreTokens()) { String toProcess = itr.nextToken();
		 * 
		 * word.set(toProcess); map.put(new Text(filename), new IntWritable(1));
		 * context.write(word, map);
		 * 
		 * }
		 */

	}
}
