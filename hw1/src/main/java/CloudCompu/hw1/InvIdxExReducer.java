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

	public void reduce(Text key, Iterable<LongArrayWritable> values,
			Context context) throws IOException, InterruptedException {
		HashMap<String, LinkedList<LongWritable>> tmpMap = new HashMap<String, LinkedList<LongWritable>>();
		// Get number of files for further usage
		long N = context.getConfiguration().getLong(
				"mapreduce.input.fileinputformat.numinputfiles", 0);

		String detString = tmpMap.size() + " > ";

		for (LongArrayWritable val : values) {
			LongWritable[] offsets = (LongWritable[]) val.toArray();
			detString = detString + " " + N + " " + val.getFileId() + " "
					+ offsets.length + " [";
			Arrays.sort(offsets);
			for (LongWritable o : offsets) {
				detString = detString + o.get() + ",";
			}
			detString = detString + "]; ";

		}

		/*
		 * for (Entry<String, Integer> entry : tmpMap.entrySet()) { detString =
		 * detString + entry.getKey() + " : " + entry.getValue() + ", "; }
		 */
		detail.set(detString);
		context.write(key, detail);
	}
}
