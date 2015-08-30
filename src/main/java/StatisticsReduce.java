import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for hadoop.
 */
public class StatisticsReduce extends
		Reducer<Text, IntWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text revId, Iterable<IntWritable> stats,
			Context context) throws IOException, InterruptedException {
		final int typeSize = 3;
		// Calculate the average of all the statistics
		Double avg = getStatsAvg(stats);

		// Get the type (pos/neg)
		String type = revId.toString().substring(0, typeSize);
		
		// Write the results
		context.write(new Text(type), new DoubleWritable(avg));
	}

	/**
	 * Parses the statistics & calculates the average.
	 * 
	 * @param dataIter
	 *            the stats iterator from hadoop's map class
	 * @return the average of the statistics
	 */
	private Double getStatsAvg(Iterable<IntWritable> dataIter) {
		Double sum = 0.0;
		int count = 0;

		// Calculate the average
		for (IntWritable stat : dataIter) {
			sum += stat.get();
			count++;
		}
		return sum / count;
	}
}
