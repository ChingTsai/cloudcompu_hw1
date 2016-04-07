package CloudCompu.hw1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvIdxReducer extends Reducer<Text, KeyDetial, Text, Text> {
	private Text detail = new Text();

	public void reduce(Text key, Iterable<KeyDetial> values, Context context)
			throws IOException, InterruptedException {
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		for (KeyDetial val : values) {
			Iterator iter = val.entrySet().iterator();
			String tmp_word;
			while (iter.hasNext()) {
				Map.Entry<Text, IntWritable> entry = (Map.Entry) iter.next();
				tmp_word = entry.getKey().toString();
				if (map.containsKey(tmp_word)) {
					map.put(tmp_word, map.get(tmp_word)
							+ entry.getValue().get());
				} else {
					map.put(tmp_word, entry.getValue().get());
				}

			}
		}
		String detString = map.size()+"";
		Iterator iter = map.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, Integer> entry = (Entry<String, Integer>) iter
					.next();
			detString.concat(entry.getKey() + ":" + entry.getValue());
		}
		detail.set(detString);
		context.write(key, detail);
	}
}
