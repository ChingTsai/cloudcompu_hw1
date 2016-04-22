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

public class RetvalCombi extends Reducer<WordPos, WordPos, WordPos, WordPos> {
	private WordPos wp = new WordPos();
	private WordPos KeyWeight = new WordPos();

	public void reduce(WordPos key, Iterable<WordPos> values, Context context)
			throws IOException, InterruptedException {

		String[] query = context.getConfiguration().get("query").split(" ");
		HashSet<String> h = new HashSet<String>();
		for (String q : query)
			h.add(q);
		String tmp = "";
		double score = 0D;
		for (WordPos val : values) {
			String[] str = val.toString().split(" ");

			if (h.contains(str[0])) {
				score += val.getW();
				tmp = str[1];
				for (int i = 2; i < str.length; i++) {
					tmp = tmp + " " + str[i];
				}

				tmp = tmp + "_";
			}

		}

		wp.set(tmp);
		wp.setW(score);
		KeyWeight.setW(score);
		KeyWeight.set(key.toString());
		context.write(KeyWeight, wp);

	}
}
