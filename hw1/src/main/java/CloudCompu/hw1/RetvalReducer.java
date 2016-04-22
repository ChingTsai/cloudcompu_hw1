package CloudCompu.hw1;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class RetvalReducer extends Reducer<Text, WordPos, Text, Text> {
	private Text detail = new Text();

	public void reduce(Text key, Iterable<WordPos> values, Context context)
			throws IOException, InterruptedException {

		int reducerId = context.getTaskAttemptID().getTaskID().getId();
		String detString = "";
		
		for (WordPos val : values) {
			detString = String detString + "Rank "+
		}

		String detString = tmpMap.size() + " > ";
		// Get number of files for further usage
		long total_word = context.getCounter(
				"org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS")
				.getValue();

		SortedSet<String> keys = new TreeSet<String>(tmpMap.keySet());
		for (String file : keys) {
			DoubleWritable[] l = tmpMap.get(file);
			detString = detString + " " + total_word + " " + file + " "
					+ l.length + " " + l[l.length - 1].get() + " [";
			Arrays.sort(l);
			for (int i = 0; i < l.length - 1; i++) {
				detString = detString + l[i].get() + ",";
			}

			detString = detString + "]; ";
		}

		detail.set(detString);
		context.write(key, detail);
	}
}
