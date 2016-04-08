package CloudCompu.hw1;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;

public class LongArrayWritable extends ArrayWritable {
	/*
	 * Any type of Writable should implement this function below or you will get
	 * java.lang.NoSuchMethodException X.X.X.<init>()
	 */
	public LongArrayWritable() {
		super(LongWritable.class);
	}

	public LongArrayWritable(LongWritable[] longs) {
		super(LongWritable.class);
		set(longs);
	}
}
