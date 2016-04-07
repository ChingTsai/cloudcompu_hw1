package CloudCompu.hw1;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class InvIdxPart extends Partitioner<Text, MapWritable> {
	public int getPartition(Text key, MapWritable value, int numReduceTasks) {

		if (key.charAt(0) <= 'g')
			return 0;
		else
			return 1;

	}

}
