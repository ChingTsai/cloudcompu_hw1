package CloudCompu.hw1;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RetvalCombi extends Reducer<Text, WordPos, Text, WordPos> {
	private WordPos wp = new WordPos();
	private Text word = new Text();

	public void reduce(Text key, Iterable<WordPos> values, Context context)
			throws IOException, InterruptedException {
		double score = 0D;
		String[] query = context.getConfiguration().get("query").split(" ");
		HashSet<String> h = new HashSet<String>();
		for (String q : query)
			h.add(q);
		for (WordPos val : values) {
			String[] str = val.toString().split(" ");
			String tmp = "";
			if (h.contains(str[0])) {
				score += val.getW();
				for (int i = 1; i < str.length; i++) {
					tmp = tmp + " " + str[i];
				}
				wp.set(tmp);
				wp.setW(score);
				word.set(key + "_" + score);
				context.write(word, wp);
			}

		}

	}
}
