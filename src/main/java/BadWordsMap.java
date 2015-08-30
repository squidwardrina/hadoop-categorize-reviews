import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class BadWordsMap extends
		Mapper<NullWritable, BytesWritable, Text, IntWritable> {
	private static final String ENCODING = "UTF-8";
	private static final String DELIM = "_";

	@Override
	protected void map(
			NullWritable key,
			BytesWritable file,
			Mapper<NullWritable, BytesWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		StatisticsCalculator statsCalculator = new StatisticsCalculator();

		// Get the review type from the filename
		Path filePath = ((FileSplit) context.getInputSplit()).getPath();
		String revType = getRevType(filePath);

		// Get number of bad words
		String strReview = IOUtils.toString(file.getBytes(), ENCODING);
		Integer wordsCount = statsCalculator.getBadWords(strReview);
		// Write the data to map-reduce context
		context.write(new Text(revType + DELIM + CheckType.badWords.toString()),
				new IntWritable(wordsCount));
	}

	/**
	 * Get review type from the filename.
	 * 
	 * @param filePath
	 *            path to the trip file
	 * @return the trip ID
	 */
	private String getRevType(Path filePath) {
		final int typeLetters = 3;
		String tripFileName = filePath.getName(); // get file name
		String strId = tripFileName.substring(0, typeLetters);
		return strId;
	}

}
