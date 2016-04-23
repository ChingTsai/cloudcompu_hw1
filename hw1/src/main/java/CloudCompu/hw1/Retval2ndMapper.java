package CloudCompu.hw1;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Retval2ndMapper extends Mapper<Text, WordPos, Text, WordPos>{
	private WordPos wp = new WordPos();
	private Text KeyWeight = new Text();

	public void map(Text key, WordPos value, Context context) throws IOException,
			InterruptedException {

		context.write(key, value);

	}
}
