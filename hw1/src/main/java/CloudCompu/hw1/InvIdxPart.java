package CloudCompu.hw1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class InvIdxPart extends Partitioner<Text, KeyDetial> {
	public int getPartition(Text key, KeyDetial value, int numReduceTasks) {

		if (key.charAt(0)<='g')
			return 0;
		else
			return 1;

	}


}
