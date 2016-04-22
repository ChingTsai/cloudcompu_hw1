package CloudCompu.hw1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RetvalGpCompare extends WritableComparator {
	protected RetvalGpCompare() {
		super(Text.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2) {
		Text t1 = (Text) w1;
		Text t2 = (Text) w2;
		double s1 = Double.parseDouble(t1.toString().split("_")[1]);
		double s2 = Double.parseDouble(t2.toString().split("_")[2]);
		if (s1 == s2)
			return 0;
		else if (s1 > s2)
			return 1;
		else
			return -1;
	}
}
