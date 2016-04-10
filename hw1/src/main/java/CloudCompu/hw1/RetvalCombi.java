package CloudCompu.hw1;

import java.io.IOException;
import java.util.HashMap;

import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RetvalCombi extends Reducer<Text, MapWritable, Text, MapWritable> {
	private MapWritable map = new MapWritable();

	public void reduce(Text key, Iterable<MapWritable> values, Context context)
			throws IOException, InterruptedException {
		
		for (MapWritable val : values) {
			map.putAll(val);
		}

		context.write(key, map);

	}
}
