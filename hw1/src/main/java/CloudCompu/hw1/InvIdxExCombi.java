package CloudCompu.hw1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class InvIdxExCombi extends
		Reducer<Text, MapWritable, Text, MapWritable> {
	private MapWritable map = new MapWritable();

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

		for (Entry<String, LinkedList<LongWritable>> e : tmpMap.entrySet()) {

			map.put(new Text(e.getKey()),
					new ArrayWritable(LongWritable.class, (LongWritable[]) e
							.getValue().toArray(
									new LongWritable[e.getValue().size()])));
		}
		context.write(key, map);

	}
}
