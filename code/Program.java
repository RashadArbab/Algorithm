import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
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
	public static final boolean SHOW_HEADERS = false;

	/**
	 * When testing, you may want to choose a smaller portion of the dataset. This
	 * number lets you limit it to only the first MAX_ENTRIES. Setting this over 3.5
	 * million will get all the data. IMPORTANT NOTE: this will ONLY have an effect
	 * when READ_INTO_MEMORAY is true
	 */
	// TODO: adjust this constant as necessary
	private static final int MAX_TWEETS = Integer.MAX_VALUE; 
	//public static final int MAX_TWEETS = Integer.MAX_VALUE;

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
		//findTopUsingArrayList(allTweets, 20);
		System.out.println("");
		System.out.println("TreeMap"); 
		System.out.println(""); 
        findTopUsingTreeMap(allTweets, 1500000);
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
		//Stopwatch stopwatch = new Stopwatch(); 
		stopwatch.start() ; 
		int numTweets = 0;
		
		ArrayList<TweetCount> listOfTweets = new ArrayList<TweetCount>();
		
		PriorityQueue<TweetCount> leaderBoard = new PriorityQueue<TweetCount>(n); 
		for (CSVRecord record : allTweets) {
			
			boolean addToList = true;

			if(record.isSet(8)) {
				numTweets++;
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
				
				if (leaderBoard.size() > n) { 
					leaderBoard.remove();
				}

			}

			

		}
		
		// Outside the loop, dont print anything inside the loop
		
		
		//leaderBoards = doubleSelectionSortList(leaderBoards);
		
		
		//printArrayList(leaderBoards); 
		
		
		
		stopwatch.stop(); 
		
		double time = stopwatch.getElapsedSeconds(); 
		
		print(leaderBoard, "an" , "ArrayList" , time , numTweets ,n ) ; 
		
		
	}
	
	/**
	 * takes a priority queue and some parameters for printing the output 
	 * then puts the queue into a stack to print it in reverse
	 * then prints the stack 
	 * @param leaderBoard 
	 * @param prefix 
	 * @param type
	 * @param time
	 * @param n
	 */
	public static void print(PriorityQueue<TweetCount> leaderBoard, String prefix, 
			String type, double time, int numTweets ,int n) {

		System.out.printf("To count %,d tweets with %s %s took %f seconds. \n", numTweets ,
				prefix ,  type , time); 
		System.out.printf("The %d users by number of tweets are: \n" , n); 
		Stack<TweetCount> stack = new Stack<TweetCount>() ; 
		int size = leaderBoard.size(); 
		
		for(int i= 0; i < size ; i ++) {
			stack.push(leaderBoard.poll()); 
		}
		for (int i = 0; i < size ; i ++) {
			System.out.printf("%s had %,d tweets \n", 
					stack.peek().getScreenName() , stack.peek().getCount()); 
			stack.pop(); 
		}
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
		stopwatch.reset() ; 
		stopwatch.start(); 
		TreeMap<String, TweetCount> map = new TreeMap<String, TweetCount>() ;
		//PriorityQueue<TweetCount> pQueue = new PriorityQueue<TweetCount>() ; 
		
		int numTweets = 0; 
		for (CSVRecord  record : allTweets) {
			if (record.isSet(8)) {
				String userName = record.get(8); 
				
				if (map.isEmpty()) {
					TweetCount tweet = new TweetCount(userName , 1); 
					map.put(userName, tweet);
					
					//pQueue.add(tweet); 
				}
				else {
					TweetCount check = map.get(userName); 
					if (check == null) {
						TweetCount tweet = new TweetCount(userName, 1);
						map.put(userName, tweet); 
						//leaderBoard.put(tweet.getCount(), tweet);
						//pQueue.add(tweet); 
					}
					else {
						//pQueue.remove(check); 
						check.increment();
						//pQueue.add(check); 
					}
				}
				
				
				
				numTweets++ ; 
			}
			
			//if (pQueue.size() > n) {
				//pQueue.remove(); 
			//}
		}
		
		/**
		 * this will create a cascading tree so if the leaderboard tree 
		 * already has a value then it will create a tree within a tree that will take more users in it then you can have 
		 * infinite users with the same value 
		 */
		System.out.println("Second way of printing"); 
		TreeMap<Integer, TreeMap<String, TweetCount>> leaderBoard = new TreeMap<Integer, TreeMap<String , TweetCount>>() ; 
		Collection<TweetCount> keys = map.values(); 
		for (TweetCount key : keys) {
			int value = key.getCount() ;
			//System.out.println(value); 
			if (!leaderBoard.containsKey(key.getCount())) {
				TreeMap<String, TweetCount> tree = new TreeMap<String, TweetCount>() ; 
				tree.put(key.getScreenName(), key); 
				leaderBoard.put(key.getCount(), tree); 
				//System.out.println("CheckPoint-2"); 
			}
			else {
				//System.out.println("CheckPoint -1"); 
				leaderBoard.get(key.getCount()).put(key.getScreenName() , key); 
			}
		}
		System.out.println("Checkpoint 0"); 
		int counter = 0; 
		Collection<TreeMap<String, TweetCount>> topUsers = leaderBoard.descendingMap().values();  
		for(TreeMap<String , TweetCount> key : topUsers) {
			Collection<TweetCount> users = key.values() ; 
 			//System.out.println("CheckPoint 1"); 
			for (TweetCount user : users) {
				System.out.println(counter + ": " + user.getScreenName() + " tweeted " + user.getCount() + " times");
				counter++; 
				if (counter >= n) {
					break; 
				}
			}
			
			if (counter >= n ) {
				break; 
			}
			
		}
		
	
		
		stopwatch.stop() ; 
		double time = stopwatch.getElapsedSeconds(); 
		//print(pQueue , "a" , "treeMap" , time, numTweets, n); 
		System.out.println(time); 
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
