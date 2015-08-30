import java.util.HashMap;
import java.util.Map;

public class AverageStats {
	private Map<Category, Double> wordsCount;
	private Map<Category, Double> goodWords;
	private Map<Category, Double> badWords;
	
	private static AverageStats instance = null;
	
	public static AverageStats getInstance() {
		if (instance == null) {
			instance = new AverageStats();
		}
		return instance;
	}
	
	private AverageStats() {
		wordsCount = new HashMap<Category, Double>();
		goodWords = new HashMap<Category, Double>();
		badWords = new HashMap<Category, Double>();
	}

	public Map<Category, Double> getWordsCount() {
		return wordsCount;
	}
	
	public Map<Category, Double> getGoodWords() {
		return goodWords;
	}
	
	public Map<Category, Double> getBadWords() {
		return badWords;
	}

	public void setWordsCount(Double pos, Double neg) {
		wordsCount.put(Category.pos, pos);
		wordsCount.put(Category.neg, neg);
	}

	public void setGoodWords(Double pos, Double neg) {
		goodWords.put(Category.pos, pos);
		goodWords.put(Category.neg, neg); 
	}
	
	public void setBadWords(Double pos, Double neg) {
		badWords.put(Category.pos, pos);
		badWords.put(Category.neg, neg); 
	}
}
