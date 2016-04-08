package CloudCompu.hw1;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvIdxExMapper extends
		Mapper<LongWritable, Text, Text, MapWritable> {
	private MapWritable map = new MapWritable();
	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		String filename = fileSplit.getPath().getName();
		HashMap<String, LinkedList<LongWritable>> tmpMap = new HashMap<String, LinkedList<LongWritable>>();
		// Replace nonAlphabetic with space
		// StringTokenizer itr = new
		// StringTokenizer(value.toString().replaceAll("[^a-zA-Z]", " "));
		Matcher matcher = Pattern.compile("\\S+").matcher(
				value.toString().replaceAll("[^a-zA-Z]", " "));
		while (matcher.find()) {
			if (tmpMap.containsKey(matcher.group())) {
				tmpMap.get(matcher.group()).add(
						new LongWritable(matcher.start()));
			} else {
				LinkedList<LongWritable> l = new LinkedList<LongWritable>();
				l.add(new LongWritable(key.get() + matcher.start()));
				tmpMap.put(matcher.group(), l);
			}
			// System.out.println(matcher.start() + ":" + matcher.group());
		}
		/*
		 * while (itr.hasMoreTokens()) { String toProcess = itr.nextToken();
		 * 
		 * word.set(toProcess); map.put(new Text(filename), new IntWritable(1));
		 * context.write(word, map);
		 * 
		 * }
		 */

		for (Entry<String, LinkedList<LongWritable>> e : tmpMap.entrySet()) {
			word.set(e.getKey());
			map.put(new Text(filename),
					new LongArrayWritable((LongWritable[]) e.getValue()
							.toArray(new LongWritable[e.getValue().size()])));
			context.write(word, map);
		}

	}
}
