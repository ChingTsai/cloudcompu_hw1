package CloudCompu.hw1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class InvIdxCompare extends WritableComparator {
	public InvIdxCompare() {
		super(Text.class, true);
	}

	public int compare(WritableComparable o1, WritableComparable o2) {

		return -1 * (o1.toString().compareTo(o2.toString()));

	}
}
