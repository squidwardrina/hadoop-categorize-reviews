import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Hadoop mapper.
 */
public class CategorizeMap extends
		Mapper<NullWritable, BytesWritable, Text, Text> {
	private static final String ENCODING = "UTF-8";

	@Override
	protected void map(NullWritable key, BytesWritable file,
			Mapper<NullWritable, BytesWritable, Text, Text>.Context context)
			throws IOException, InterruptedException {
		StatisticsCalculator statsCalculator = new StatisticsCalculator();
		FileSplit fSplit = (FileSplit) context.getInputSplit();
		Category result, resultWords, resultGood, resultBad;

		// Get review name from the file name
		String reviewName = fSplit.getPath().getName();
		Integer wordsCount;
		Map<Category, Double> avgWords;
		String strReview = IOUtils.toString(file.getBytes(), ENCODING);

		// Get rev's words & avg words
		wordsCount = statsCalculator.getWordsCount(strReview);
		avgWords = AverageStats.getInstance().getWordsCount();
		resultWords = getClosestType(wordsCount, avgWords); // get closest type

		// Get rev's good words & avg good words
		wordsCount = statsCalculator.getGoodWords(strReview);
		avgWords = AverageStats.getInstance().getGoodWords();
		resultGood = getClosestType(wordsCount, avgWords); // get closest type

		// Get rev's bad words & avg bad words
		wordsCount = statsCalculator.getBadWords(strReview);
		avgWords = AverageStats.getInstance().getBadWords();
		resultBad = getClosestType(wordsCount, avgWords); // get closest type

		// Get the final result
		result = getFinalResult(resultWords, resultGood, resultBad);

		// Write result
		context.write(new Text(reviewName), new Text(result.toString()));
	}

	private Category getFinalResult(Category resultWords, Category resultGood,
			Category resultBad) {
		Category result;
		int countPositive = 0;
		if (resultWords == Category.pos) {
			countPositive++;
		}
		if (resultGood == Category.pos) {
			countPositive++;
		}
		if (resultBad == Category.pos) {
			countPositive++;
		}
		if (countPositive >= 2) {
			result = Category.pos;
		} else {
			result = Category.neg;
		}
		return result;
	}

	private Category getClosestType(Integer wordsCount,
			Map<Category, Double> avgWords) {
		Category result;
		Double distPos = Math.abs(avgWords.get(Category.pos) - wordsCount);
		Double distNeg = Math.abs(avgWords.get(Category.neg) - wordsCount);
		if (distPos > distNeg) {
			result = Category.neg;
		} else {
			result = Category.pos;
		}
		return result;
	}
}
