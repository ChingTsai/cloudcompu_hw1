package CloudCompu.hw1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RetvalGpCompare extends WritableComparator {
	protected RetvalGpCompare() {
		super(WordPos.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2) {
		WordPos t1 = (WordPos) w1;
		WordPos t2 = (WordPos) w2;
		return t1.toString().compareTo(t2.toString());
	}
}
