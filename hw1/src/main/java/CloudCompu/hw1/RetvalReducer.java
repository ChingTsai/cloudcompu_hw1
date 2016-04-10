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

public class RetvalReducer extends Reducer<Text, MapWritable, Text, Text> {
	private Text detail = new Text();

	public void reduce(Text key, Iterable<MapWritable> values, Context context)
			throws IOException, InterruptedException {
		HashMap<String, DoubleWritable[]> tmpMap = new HashMap<String, DoubleWritable[]>();

		for (MapWritable val : values) {
			Iterator<Entry<Writable, Writable>> iter = val.entrySet().iterator();
			String tmpWord;
			while (iter.hasNext()) {

				Map.Entry<Text, ArrayWritable> entry = (Map.Entry) iter.next();
				DoubleWritable[] l = (DoubleWritable[]) entry.getValue().toArray();
				tmpWord = entry.getKey().toString();
				tmpMap.put(tmpWord, l);

			}
		}

		String detString = tmpMap.size() + " > ";
		// Get number of files for further usage
		long total_word = context.getCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue();

		SortedSet<String> keys = new TreeSet<String>(tmpMap.keySet());
		for (String file : keys) {
			DoubleWritable[] l = tmpMap.get(file);
			detString = detString + " " + total_word + " " + file + " " + l.length + " " + l[l.length - 1].get() + " [";
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
