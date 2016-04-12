package CloudCompu.hw1;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvIdxExReducer extends Reducer<Text, MapWritable, Text, Text> {
	private Text detail = new Text();
	private Text word = new Text();

	public void reduce(Text key, Iterable<LongArrayWritable> values,
			Context context) throws IOException, InterruptedException {
		// Get number of files for further usage
		long N = context.getConfiguration().getLong(
				"mapreduce.input.fileinputformat.numinputfiles", 0);

		String detString = " "+ N;
		int df = 0;

		for (LongArrayWritable val : values) {
			LongWritable[] offsets = (LongWritable[]) val.toArray();
			detString = detString + " " + val.getFileId() + " "
					+ offsets.length + " [";
			Arrays.sort(offsets);
			for (LongWritable o : offsets) {
				detString = detString + o.get() + ",";
			}
			detString = detString + "]; ";
			df++;

		}
		detString = df + " > " + detString;
		/*
		 * for (Entry<String, Integer> entry : tmpMap.entrySet()) { detString =
		 * detString + entry.getKey() + " : " + entry.getValue() + ", "; }
		 */
		detail.set(detString);
		word.set(key.toString().split("_")[0]);
		context.write(word, detail);
	}
}
