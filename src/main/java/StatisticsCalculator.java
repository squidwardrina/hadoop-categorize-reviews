import java.util.ArrayList;
import java.util.List;


public class StatisticsCalculator {
	private List<String> goodWordsBeginnings = null;
	private List<String> badWordsBeginnings = null;
	
	public StatisticsCalculator() {
		goodWordsBeginnings = new ArrayList<String>();
		goodWordsBeginnings.add("interest");
		goodWordsBeginnings.add("enjoy");
		goodWordsBeginnings.add("astonish");
		goodWordsBeginnings.add("perfect");
		goodWordsBeginnings.add("fabulous");
		goodWordsBeginnings.add("extraordinary");
		goodWordsBeginnings.add("masterpiece");
		goodWordsBeginnings.add("magical");
		goodWordsBeginnings.add("quality");
		goodWordsBeginnings.add("great");
		goodWordsBeginnings.add("wonderful");
		goodWordsBeginnings.add("everything");
		goodWordsBeginnings.add("art");
		goodWordsBeginnings.add("clever");
		goodWordsBeginnings.add("beautiful");
		goodWordsBeginnings.add("love");
		goodWordsBeginnings.add("surprise");
		goodWordsBeginnings.add("intelligent");
		goodWordsBeginnings.add("thought provoking");
		goodWordsBeginnings.add("ideal");
		goodWordsBeginnings.add("professional");
		goodWordsBeginnings.add("creativ");		

		badWordsBeginnings = new ArrayList<String>();
		badWordsBeginnings.add("terribl");
		badWordsBeginnings.add("boring");
		badWordsBeginnings.add("problem");
		badWordsBeginnings.add("movies like th");
		badWordsBeginnings.add("awful");
		badWordsBeginnings.add("bored");
		badWordsBeginnings.add("pathetic");
		badWordsBeginnings.add("dull");
		badWordsBeginnings.add("trash");
		badWordsBeginnings.add("flat");
		badWordsBeginnings.add("dumb");
		badWordsBeginnings.add("nothing");
		badWordsBeginnings.add("disappoint");
		badWordsBeginnings.add("waste");
	}
	
	public Integer getWordsCount(String strReview) {
		int wordCount = 0;

		boolean word = false;
		int endOfLine = strReview.length() - 1;

		for (int i = 0; i < strReview.length(); i++) {
			// if the char is a letter, word = true.
			if (Character.isLetter(strReview.charAt(i)) && i != endOfLine) {
				word = true;
				// if char isn't a letter and there have been letters before,
				// counter goes up.
			} else if (!Character.isLetter(strReview.charAt(i)) && word) {
				wordCount++;
				word = false;
				// last word of the review; if it doesn't end with a non letter,
				// it wouldn't count without this.
			} else if (Character.isLetter(strReview.charAt(i))
					&& i == endOfLine) {
				wordCount++;
			}
		}
		return wordCount;		
	}

	public Integer getGoodWords(String strReview) {
		int wordCount = 0;

		for (String goodWord : goodWordsBeginnings) {
			if (strReview.contains(goodWord)) {
				wordCount++;
			}
		}
		
		return wordCount;		
	}
	
	public Integer getBadWords(String strReview) {
		int wordCount = 0;

		for (String badWord : badWordsBeginnings) {
			if (strReview.contains(badWord)) {
				wordCount++;
			}
		}
		
		return wordCount;		
	}
}
