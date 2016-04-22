package CloudCompu.hw1;

import java.io.IOException;
import java.nio.charset.Charset;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RetvalReducer extends Reducer<Text, WordPos, Text, Text> {
	private Text detail = new Text();

	public void reduce(Text key, Iterable<WordPos> values, Context context) throws IOException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String inputDir = context.getConfiguration().get("inputDir");
		FileStatus[] status_list = fs.listStatus(new Path(inputDir));

		int reducerId = context.getTaskAttemptID().getTaskID().getId();
		String detString = "";
		int file_id;
		int subRank = 0;
		Path inFile;
		byte[] buffer = new byte[1024];
		for (WordPos val : values) {
			file_id = Integer.parseInt(key.toString().split("_")[0]);
			detString = detString + "Rank " + subRank + reducerId + ": " + status_list[file_id] + " score = "
					+ Double.toString(val.getW()) + "\r\n";
			detString = detString + "************************\r\n";
			subRank++;
			inFile = new Path(inputDir + "/" + status_list[file_id]);
			for (String o : val.toString().split(" ")) {
				fs.open(inFile).read(Long.parseLong(o) - 50L, buffer, 0, 1024);
				detString = detString + new String(buffer, Charset.forName("UTF-8"));
			}
			detString = detString + "************************\r\n";
		}

		detail.set(detString);
		context.write(key, detail);
	}
}
