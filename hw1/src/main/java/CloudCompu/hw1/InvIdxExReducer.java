package CloudCompu.hw1;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvIdxExReducer extends Reducer<Text, MapWritable, Text, Text> {
	private Text detail = new Text();

	public void reduce(Text key, Iterable<MapWritable> values, Context context)
			throws IOException, InterruptedException {
		HashMap<String, LinkedList<LongWritable>> tmpMap = new HashMap<String, LinkedList<LongWritable>>();

		for (MapWritable val : values) {
			Iterator<Entry<Writable, Writable>> iter = val.entrySet()
					.iterator();
			String tmpWord;
			while (iter.hasNext()) {

				Map.Entry<Text, ArrayWritable> entry = (Map.Entry) iter.next();
				LongWritable[] l = (LongWritable[]) entry.getValue().toArray();
				tmpWord = entry.getKey().toString();
				if (!tmpMap.containsKey(tmpWord))
					tmpMap.put(tmpWord, new LinkedList<LongWritable>());

				for (LongWritable offset : l) {
					tmpMap.get(tmpWord).add(offset);
				}

			}
		}
		
		String detString = tmpMap.size() + " -> ";
		long N = context.getConfiguration().getLong("mapreduce.input.fileinputformat.numinputfiles", 0);

		
		SortedSet<String> keys = new TreeSet<String>(tmpMap.keySet());
		for (String file : keys) {
			detString = detString +" "+N+" "+ file + " " + tmpMap.get(file).size() + " [";
			Collections.sort(tmpMap.get(file));
			for (LongWritable offset : tmpMap.get(file)) {
				detString = detString + offset.get() + ",";
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
