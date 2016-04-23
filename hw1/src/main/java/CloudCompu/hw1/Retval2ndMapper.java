package CloudCompu.hw1;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Retval2ndMapper extends Mapper<Text, Text, Text, WordPos>{
	private WordPos KeyWeight = new WordPos();

	public void map(Text key, WordPos value, Context context) throws IOException,
			InterruptedException {
		String[] s = value.toString().split("~");
		KeyWeight.set(s[1]);
		KeyWeight.setW(Double.parseDouble(key.toString()));
		KeyWeight.setfile_id(Integer.parseInt(s[0]));
		context.write(key, KeyWeight);

	}
}
