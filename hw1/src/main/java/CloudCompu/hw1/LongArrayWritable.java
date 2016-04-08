package CloudCompu.hw1;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;

public class LongArrayWritable extends ArrayWritable {

	public LongArrayWritable() {
		super(LongWritable.class);
	}

	public LongArrayWritable(LongWritable[] longs) {
		super(LongWritable.class);
		set(longs);
	}
}
