import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for hadoop.
 */
public class CategorizeReduce extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text reviewName, Iterable<Text> result, Context context)
			throws IOException, InterruptedException {
		context.write(reviewName, result.iterator().next());
	}
}