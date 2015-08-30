import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver for hadoop. Determines whether a movie review is positive or negative.
 */
public class Driver {
	private static final String OUT_FOLDER = "tempOutput";
	private static final String WORDS_PATH = CheckType.wordsCount.toString();
	private static final String GOOD_WORDS_PATH = CheckType.goodWords.toString();
	private static final String BAD_WORDS_PATH = CheckType.badWords.toString();
	private static final String EXAMPLES = "examples";
	private static final String INPUT = "input";
	private static final String OUT_FILE = "output.txt";

	/**
	 * Determines whether a movie review is positive or negative.
	 * 
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// Get average statistics 
		analyzeStats(WordsCountMap.class, WORDS_PATH);
		analyzeStats(GoodWordsMap.class, GOOD_WORDS_PATH);
		analyzeStats(BadWordsMap.class, BAD_WORDS_PATH);

		setAverageStats();
		
		categorizeInput(); // Pick the false trips
	}

	/**
	 * Clears hadoop's unnecessary generated files and renames the result file
	 * as needed.
	 * 
	 * @param outFile
	 *            the output directory
	 */
	private static void outputToFile(String outFile) {
		File outputFolder = new File(OUT_FOLDER);
		File[] generatedFiles = outputFolder.listFiles();

		// Go over the files in the output directory
		for (File file : generatedFiles) {
			String filename = file.getName();

			// Delete all the unnecessary generated files
			if (filename.startsWith(".") || filename.startsWith("_")) {
				file.delete();
			} else {
				// Rename the result file
				file.renameTo(new File(outFile));
			}
		}

		// Delete the directory
		outputFolder.delete();
	}

	/**
	 * Remove all the created temporary files.
	 * 
	 * @throws IOException
	 */
	private static void removeTempFiles() throws IOException {
		FileUtils.deleteDirectory(new File(WORDS_PATH));
		FileUtils.deleteDirectory(new File(GOOD_WORDS_PATH));
		FileUtils.deleteDirectory(new File(BAD_WORDS_PATH));
	}

	/**
	 * Performs the map reduce behavior analyze job.
	 * 
	 * @param inPath
	 *            input path
	 * @param mapper
	 *            mapper class
	 * @param outPath
	 *            path to save the operation results
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private static void analyzeStats(
			Class<? extends Mapper<?, ?, ?, ?>> mapper, String resPath)
			throws IOException, InterruptedException, ClassNotFoundException {
		// Configure new job
		Configuration conf = new Configuration();
		Job analyzerJob = new Job(conf, "Analyze Review Statistics");
		analyzerJob.setJarByClass(Driver.class);

		// Mapper settings:
		analyzerJob.setMapperClass(mapper);
		analyzerJob.setInputFormatClass(WholeFileInputFormat.class);
		analyzerJob.setMapOutputKeyClass(Text.class);
		analyzerJob.setMapOutputValueClass(IntWritable.class);
		WholeFileInputFormat.addInputPath(analyzerJob, new Path(EXAMPLES));

		// Reducer settings:
		analyzerJob.setReducerClass(StatisticsReduce.class);
		analyzerJob.setOutputKeyClass(Text.class);
		analyzerJob.setOutputValueClass(DoubleWritable.class);
		FileOutputFormat.setOutputPath(analyzerJob, new Path(resPath));

		analyzerJob.waitForCompletion(true);
	}
	
	private static Pair<Double, Double> getAvg(String path) {
		Double pos = null, neg = null;
		List<String> lines = null;
		// Get the avg words counts for reviews
		try {
			lines = Files.readAllLines(Paths.get(path + "/part-r-00000"));
		} catch (IllegalArgumentException | IOException e) {
			e.printStackTrace();
		}
	
		// Get good / bad avg
		for (String line : lines) {
			if (line.startsWith(Category.pos.toString())) {
				pos = Double.parseDouble(line.substring(line.indexOf("\t") + 1));
			} else {
				neg = Double.parseDouble(line.substring(line.indexOf("\t") + 1));
			}
		}		
		return new Pair<Double, Double>(pos, neg);
	}
	
	private static void setAverageStats() {
		AverageStats avgStats = AverageStats.getInstance();
		Pair<Double, Double> stats;

		// Get words count stats
		stats = getAvg(WORDS_PATH);		
		avgStats.setWordsCount(stats.getFirst(), stats.getSecond());

		// Get good words count stats
		stats = getAvg(GOOD_WORDS_PATH);		
		avgStats.setGoodWords(stats.getFirst(), stats.getSecond());

		// Get bad words count stats
		stats = getAvg(BAD_WORDS_PATH);		
		avgStats.setBadWords(stats.getFirst(), stats.getSecond());
	}

	/**
	 * Performs the job which categorizes the input.
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	private static void categorizeInput() throws IOException,
			InterruptedException, ClassNotFoundException {
		// Configure new job
		Configuration conf = new Configuration();
		Job sJob = new Job(conf, "Get false trips");
		sJob.setJarByClass(Driver.class);

		// Mapper settings:
		sJob.setMapperClass(CategorizeMap.class);
		sJob.setInputFormatClass(WholeFileInputFormat.class);
		WholeFileInputFormat.addInputPath(sJob, new Path(INPUT));
		sJob.setMapOutputKeyClass(Text.class);
		sJob.setMapOutputValueClass(Text.class);

		// Reducer settings:
		sJob.setReducerClass(CategorizeReduce.class);
		sJob.setOutputKeyClass(Text.class);
		sJob.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(sJob, new Path(OUT_FOLDER));

		sJob.waitForCompletion(true);

		removeTempFiles(); // Clear the program's doodles

		outputToFile(OUT_FILE); // get the output to spec. file
	}
}
