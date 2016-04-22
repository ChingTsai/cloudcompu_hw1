package CloudCompu.hw1;

import java.io.IOException;
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RetvalMapper extends Mapper<Text, Text, Text, WordPos> {
	private WordPos wp = new WordPos();
	private WordPos KeyWeight = new WordPos();

	public void map(Text key, Text value, Context context) throws IOException,
			InterruptedException {
		int N = Integer.parseInt(context.getConfiguration().get("N"));
		// Replace nonAlphabetic with space

		StringTokenizer itr = new StringTokenizer(value.toString().replaceAll(
				"[\\Q][,;>\\E]", " "));

		int df, tf;

		String fileName;
		df = Integer.parseInt(itr.nextToken());

		for (int i = 0; i < df; i++) {
			String offset = "";
			fileName = itr.nextToken();
			tf = Integer.parseInt(itr.nextToken());

			for (int j = 0; j < tf; j++) {
				offset = offset + " " + itr.nextToken();
			}

			wp.setW((double) tf * Math.log10((double) N / (double) df));
			wp.set(key.toString()+" "+offset);
			KeyWeight.set(fileName);

			context.write(KeyWeight, wp);
		}

	}
}
