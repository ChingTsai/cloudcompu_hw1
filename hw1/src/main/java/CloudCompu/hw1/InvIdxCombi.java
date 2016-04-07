package CloudCompu.hw1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvIdxCombi extends Reducer<Text, KeyDetial, Text, KeyDetial> {
	private KeyDetial kd = new KeyDetial();
	private Text word = new Text();
	private Text file = new Text();
	private IntWritable count = new IntWritable();

	public void reduce(Text key, Iterable<KeyDetial> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (KeyDetial val : values) {
			sum += val.getWordCount();
		}
		String[] fileWord = key.toString().split("_");
		word.set(fileWord[0]);
		file.set(fileWord[1]);
		count.set(sum);

		map.put(file, count);
		context.write(word, map);

	}
}
