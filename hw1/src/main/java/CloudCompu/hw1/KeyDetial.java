package CloudCompu.hw1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class KeyDetial extends MapWritable {
	private int wordCount = 0;
	private Map<Writable, Writable> instance;

	public void setWordCount(int count) {
		this.wordCount = count;
	}

	public int getWordCount() {
		return this.wordCount;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		out.writeInt(wordCount);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);

		wordCount = in.readInt();
	}

}
