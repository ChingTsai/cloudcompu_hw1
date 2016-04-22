package CloudCompu.hw1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RetvalSortCompare extends WritableComparator {
	protected RetvalSortCompare() {
		super(WordPos.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2) {
		WordPos t1 = (WordPos) w1;
		WordPos t2 = (WordPos) w2;

		Double d1 = t1.getW();
		Double d2 = t2.getW();
		return Double.compare(d1, d2);
	}
}
