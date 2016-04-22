package CloudCompu.hw1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

public class WordPos extends Text {
	private double w = 0;

	public void setW(double W) {
		w = W;
	}

	public double getW() {
		return w;
	}

	public int compareTo(WordPos other) {
		return this.toString().compareTo(other.toString());
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		out.writeDouble(w);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);

		w = in.readDouble();
	}
}
