package CloudCompu.hw1;

import java.io.IOException;
import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RetvalMapper extends Mapper<Text, Text, Text, MapWritable> {
	private MapWritable map = new MapWritable();
	private Text word = new Text();

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		// Replace nonAlphabetic with space

		StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("[\\Q][,;>\\E]", " "));

		int df, tf, N;

		LinkedList<DoubleWritable> l = new LinkedList<DoubleWritable>();
		String fileName;
		df = Integer.parseInt(itr.nextToken());
		for (int i = 0; i < df; i++) {
			N = Integer.parseInt(itr.nextToken());
			fileName = itr.nextToken();
			tf = Integer.parseInt(itr.nextToken());
			l.clear();
			for (int j = 0; j < tf; j++) {
				l.add(new DoubleWritable(Long.parseLong(itr.nextToken())));
			}
			l.add(new DoubleWritable((double) tf * Math.log((double) N / (double) df)));
			word.set(fileName);
			map.put(new Text(key.toString()),
					new DoubleArrayWritable((DoubleWritable[]) l.toArray(new DoubleWritable[l.size()])));
			context.write(word, map);
		}

	}
}
