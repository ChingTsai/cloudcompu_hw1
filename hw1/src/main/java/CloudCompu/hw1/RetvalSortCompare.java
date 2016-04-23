package CloudCompu.hw1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RetvalSortCompare extends WritableComparator {
	protected RetvalSortCompare() {
		super(Text.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2) {
		Text t1 = (Text) w1;
		Text t2 = (Text) w2;

		return Double.compare(Double.parseDouble(t1.toString().split("_")[1]),
				Double.parseDouble(t2.toString().split("_")[1]));
	}
}
