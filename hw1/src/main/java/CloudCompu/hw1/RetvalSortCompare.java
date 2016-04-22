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

		int compare = t1.toString().compareTo(t2.toString());
		if (compare == 0) {
			Double d1 = t1.getW();
			Double d2 = t2.getW();
			if(d1.equals(d2))
				return 0;
			else if(d1 > d2)
				return 1;
			else
				return -1;
		}
		return compare;
	}
}
