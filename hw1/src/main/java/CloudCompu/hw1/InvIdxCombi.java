package CloudCompu.hw1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvIdxCombi extends Reducer<Text, KeyDetial, Text, KeyDetial> {
	private KeyDetial kd = new KeyDetial();
	private Text word = new Text();
	private Text file = new Text();
	private IntWritable count = new IntWritable();

	public void reduce(Text key, Iterable<KeyDetial> values, Context context)
			throws IOException, InterruptedException {

		Iterator<KeyDetial> iter = values.iterator();
		KeyDetial kdHead = iter.next();
		if (kdHead.getWordCount() == 0) {
			//Already Combined, Fast Pass
			context.write(key, kdHead);
		} else {
			int sum = 0;
			sum += kdHead.getWordCount();
			while(iter.hasNext()){
				sum += iter.next().getWordCount();
			}
			String[] fileWord = key.toString().split("_");
			word.set(fileWord[0]);
			file.set(fileWord[1]);
			count.set(sum);

			kd.put(file, count);
			context.write(word, kd);
		}
	}
}
