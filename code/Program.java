import java.io.FileReader;
import java.util.ArrayList;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.commons.collections4.iterators.IteratorChain;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class Program {
	public static final String TRUMP_TWEETS_FILENAME = "hashtag_donaldtrump.csv";
	public static final String BIDEN_TWEETS_FILENAME = "hashtag_joebiden.csv";

	/**
	 * This constant will allow you to either read the data into local memory
	 * (_your_ code will be faster, but won't work in Codio for the entire dataset)
	 * or to use persistent storage (_your_ code will be slower, but uses a smaller
	 * amount of memory and is possible to complete entirely in Codio).
	 */
	// TODO: adjust this constant as necessary
	public static final boolean READ_INTO_MEMORY = true;

	/**
	 * This constant will allow you to display the header information for the data
	 * files (i.e., what columns have what names). Set this to false before handing
	 * in the assignment.
	 */
	// TODO: adjust this constant as necessary
	public static final boolean SHOW_HEADERS = true;

	/**
	 * When testing, you may want to choose a smaller portion of the dataset. This
	 * number lets you limit it to only the first MAX_ENTRIES. Setting this over 3.5
	 * million will get all the data. IMPORTANT NOTE: this will ONLY have an effect
	 * when READ_INTO_MEMORAY is true
	 */
	// TODO: adjust this constant as necessary
	// private static final int MAX_TWEETS = 50000;
	public static final int MAX_TWEETS = Integer.MAX_VALUE;

	/**
	 * This constant represents the folder that contains the data. You should not
	 * have to adjust this.
	 */
	public static final String DATA_DIRECTORY = "data";

	/**
	 * This static field manages timing in your code. You can and should reuse it to
	 * time your code.
	 */
	public static Stopwatch stopwatch = new Stopwatch();

	/**
	 * The main method of your program. The first half of this is written for you
	 * (don't adjust this!). Where it says "Add your code here..." you should put
	 * your main program (remember to use methods where appropriate).
	 * 
	 * @param args the arguments to this main program provided on the command line
	 *             (none)
	 * @throws IOException when the data files cannot be read properly
	 */
	public static void main(String[] args) throws IOException {
		/*
		 * We have already setup all the data to make the rest of your assignment easier
		 * for you.
		 * 
		 * All of the data is available in an Iterable object. While you may not know
		 * what an Iterable object is, you've already used these many times before!
		 * ArrayList, LinkedList, the keySet for a HashMap, etc. are all Iterable
		 * objects. To use them, you just need to use a for each loop.
		 * 
		 * IMPORTANT NOTE: the big difference between what you might be used to and the
		 * Iterable is that YOU CAN ONLY ITERATE OVER IT ONE TIME. Once you've done
		 * that, the Iterable object will be "exhausted". Therefore, make sure in that
		 * first pass that you store the information you need in the appropriate Map
		 * object (as per the assignment instructions).
		 */
		Path dir = Paths.get(DATA_DIRECTORY);

		Iterable<CSVRecord> trumpTweets = readData(dir.resolve(TRUMP_TWEETS_FILENAME), "Trump tweets", READ_INTO_MEMORY,
				false);

		Iterable<CSVRecord> bidenTweets = readData(dir.resolve(BIDEN_TWEETS_FILENAME), "Biden tweets", READ_INTO_MEMORY,
				SHOW_HEADERS);

		Iterable<CSVRecord> allTweets = () -> new IteratorChain<>(trumpTweets.iterator(), bidenTweets.iterator());

		/*
		 ************************ IMPORTANT *********************
		 * 
		 * NOTE: You may NOT change anything about these lists! You should only read
		 * stats from index 0 to index list.size() - 1 Do NOT sort the list or change
		 * them in any way.
		 * 
		 ********************************************************/

		// TODO: you can comment/uncomment to test each map
		// implementation.
		findTopUsingArrayList(allTweets, 20);
//        findTopUsingTreeMap(allTweets, 20);
//        findTopUsingHashMap(allTweets, 20);
	}

	/**
	 * TODO: Add your code for unsorted array list here. You will do best to create
	 * many methods to organize your work and experiments.
	 * 
	 * @param allTweets the Iterable object that lets you iterate over all of the
	 *                  data
	 * @param n         the number of users to report the top N for
	 */
	public static void findTopUsingArrayList(Iterable<CSVRecord> allTweets, int n) {
		// This loop is just a demonstration of how to iterate
		// through these tweets.
		//
		// After this loop has run, there is no more access to all
		// the tweet records (e.g., you can't have a second for loop
		// on allTweets), so you'll NEED to modify this code.
		Stopwatch stopwatch = new Stopwatch(); 
		stopwatch.start() ; 
		int numTweets = 0;
		int print10 = 0;
		ArrayList<TweetCount> listOfTweets = new ArrayList<TweetCount>();
		
		PriorityQueue<TweetCount> leaderBoard = new PriorityQueue<TweetCount>(20); 
		for (CSVRecord record : allTweets) {
			numTweets++;
			boolean addToList = true;

			if (print10 <= 100) {

				String userName = record.get(8);
				TweetCount tweet = new TweetCount(userName, 1);
				// boolean done = false;
				if (listOfTweets.isEmpty()) {
					listOfTweets.add(tweet);
					leaderBoard.add(tweet); 
					
					//leaderBoards = leaderBoard(leaderBoards, tweet);
				} else {

					for (TweetCount tc : listOfTweets) {
						if (tc.getScreenName().equals(userName)) {
							
							leaderBoard.remove(tc); 
							tc.increment();
							//leaderBoards = leaderBoard(leaderBoards, tc);
							leaderBoard.add(tc); 
							
							addToList = false;
							
						
							break;
						}
					}

					if (addToList == true) {
						listOfTweets.add(tweet);
						//leaderBoards = leaderBoard(leaderBoards, tweet);
						leaderBoard.add(tweet); 
						
					}

				}
				
				if (leaderBoard.size() > 20) { 
					leaderBoard.remove();
				}

			}

			print10++;

		}
		
		// Outside the loop, dont print anything inside the loop
		
		
		//leaderBoards = doubleSelectionSortList(leaderBoards);
		
		
		//printArrayList(leaderBoards); 
		
		int size = leaderBoard.size(); 
		for(int i= 0; i < size ; i ++) {
			TweetCount currentTweet = leaderBoard.poll(); 
			System.out.println("ScreenName: " + currentTweet.getScreenName() + "   Count: " 
			+ currentTweet.getCount() + "    size: " + leaderBoard.size()); 
		}
		
		stopwatch.stop(); 
		
		double time = stopwatch.getElapsedSeconds(); 
		
		System.out.println("Time taken: " + time) ; 
		System.out.printf("There are %,d tweets in total.\n", numTweets);
	}
	
	
	/**
	 * takes current leader board and new user checks if worthy of joining or not
	 * This function gives me an error of java.util.ConcurrentModificationException
	 * which from what i understood meant that you can't change an ArrayList on the
	 * same thread that you are using to iterate over it. otherwise i would have
	 * used this method
	 * 
	 * @param list
	 * @param tweet
	 * @return modified leader board
	 */
	public static ArrayList<TweetCount> leaderBoard(ArrayList<TweetCount> list, TweetCount tweet) {
		// Check for already being on leaderBoard
		boolean alreadyOn = false;
		for (TweetCount user : list) {
			if (tweet.getScreenName().equals(user.getScreenName())) {
				alreadyOn = true;
			}
		}
		if (alreadyOn == false) {
			if (list.size() < 20) {
				list.add(tweet);
			} else {
				int counter = 0;
				int index = 0;
				boolean insert = false;
				for (TweetCount users : list) {
					if (users.getCount() <= tweet.getCount()) {
						index = counter;
						insert = true;
						break;
					}
					counter++;
				}
				if (insert == true) {
					list.add(index, tweet);
		

					list.remove(20);
					
				}

			}

		}
		return list;

	}

	public static void printArrayList(ArrayList<TweetCount> list) {
		for (TweetCount tweet : list) {
			System.out.println(tweet.getScreenName() + "     " + tweet.getCount());
		}

	}

	/**
	 * This algorithm was too good to pass up it runs a selection sort in half the
	 * time now that is the kind of algorithm that gets the big $
	 * 
	 * @param list
	 * @return
	 */
	public static ArrayList<TweetCount> doubleSelectionSortList(ArrayList<TweetCount> list) {

		/*
		 * if (unsorted == null || unsorted.length == 0){ throw new
		 * IllegalArgumentException("Array is null or empty");
		 * 
		 * 
		 * }
		 */

		for (int i = 0; i < ((list.size()) + 1) / 2; i++) {
			int small = list.get(i).getCount();
			int indexOfSmall = i;
			int big = list.get(i).getCount();
			int indexOfBig = i;

			for (int j = i; j < list.size() - i; j++) {

				if (list.get(j).getCount() < small) {
					small = list.get(j).getCount();
					indexOfSmall = j;

				}

				if (list.get(j).getCount() > big) {
					big = list.get(j).getCount();
					indexOfBig = j;
					
				}

			}

			if (list.get(i).getCount() != list.get(indexOfBig).getCount()) {
				
				list = swapAL(list, i, indexOfBig);
				if (i == indexOfSmall) {
					indexOfSmall = indexOfBig;
				}
			}
			int second = list.size() - (i + 1);
			if (list.get(second).getCount() != list.get(indexOfSmall).getCount() && list.get(second).getCount() != small) {

				
				list = swapAL(list, list.size() - (i + 1), indexOfSmall);
			}
			// System.out.println("Run " + i + " :" + Arrays.toString(unsorted));

		}

		return list;

	}
	
	public static ArrayList<TweetCount> swapAL(ArrayList<TweetCount> list , int a, int b){
		/*
		 int swapa = a[i]; 
		 
        
        a[i] = a[j];
        a[j] = swapa; 

        return a; 
        */
		
		TweetCount temp = list.get(a); 
		
		list.set(a, list.get(b)); 
		list.set(b, temp); 
		
		return list; 
        
	}

	/**
	 * TODO: Add your code for tree maps here. You will do best to create many
	 * methods to organize your work and experiments.
	 * 
	 * @param allTweets the Iterable object that lets you iterate over all of the
	 *                  data
	 * @param n         the number of users to report the top N for
	 */
	public static void findTopUsingTreeMap(Iterable<CSVRecord> allTweets, int n) {
	}

	/**
	 * TODO: Add your code for hash maps here. You will do best to create many
	 * methods to organize your work and experiments.
	 * 
	 * @param allTweets the Iterable object that lets you iterate over all of the
	 *                  data
	 * @param n         the number of users to report the top N for
	 */
	public static void findTopUsingHashMap(Iterable<CSVRecord> allTweets, int n) {
	}

	/**
	 * YOU SHOULD NOT CHANGE THIS METHOD.
	 * 
	 * This method reads the data into an Iterable object.
	 * 
	 * @param path           the path of the file to read from
	 * @param description    the description to use when reporting the type of data
	 * @param readIntoMemory true if the data should be read into memory (it takes a
	 *                       lot of memory!), false if the Iterable object should
	 *                       just go through the file.
	 * @param printHeader    true if this method should print the header information
	 *                       (i.e., which column has what name).
	 * @return an will invesIterable object with all of the data in CSVRecord
	 *         objects
	 * @throws IOException if the file could not be read
	 */
	public static Iterable<CSVRecord> readData(Path path, String description, boolean readIntoMemory,
			boolean printHeader) throws IOException {
		stopwatch.reset();
		stopwatch.start();

		CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new FileReader(path.toFile()));
		Map<String, Integer> headerMap = parser.getHeaderMap();
		Iterable<CSVRecord> iterable = () -> parser.iterator();

		if (readIntoMemory) {
			List<CSVRecord> list2 = StreamSupport.stream(iterable.spliterator(), false).limit(MAX_TWEETS)
					.collect(Collectors.toList());

			System.out.printf("Finished reading %,d %s in %f seconds.\n", list2.size(), description,
					stopwatch.getElapsedSeconds());

			iterable = list2;
		}

		if (printHeader) {
			System.out.println("Data available:");
			for (String key : headerMap.keySet()) {
				int value = headerMap.get(key);
				System.out.printf("\t%d = %s\n", value, key);
			}
		}

		return iterable;
	}
}
