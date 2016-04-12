package CloudCompu.hw1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class InvIdxPart extends Partitioner<Text, LongArrayWritable> {
	public int getPartition(Text key, LongArrayWritable value, int numReduceTasks) {
		int c = key.charAt(0);
		int part = numReduceTasks / 2;
		if (c < 'a') {
			return part * ((c - 'A') / 26);
		} else {
			return part * ((c - 'A') / 26) + part;
		}

	}
}
