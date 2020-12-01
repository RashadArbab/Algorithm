import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
	public static final int MAX_TWEETS = 1024000;

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
	public static Stopwatch timeTreeMap = new Stopwatch() ; 
	public static Stopwatch timeHashMap = new Stopwatch() ;
	public static Stopwatch timeList = new Stopwatch() ; 
	
	public static int capacity = 0 ; 
	/**
	 * The main method of your program. The first half of this is written for you
	 * (don't adjust this!). Where it says "Add your code here..." you should put
	 * your main program (remember to use methods where appropriate).
	 * 
	 * @param args the arguments to this main program provided on the command line
	 *             (none)
	 * @throws IOException when the data files cannot be read properly
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException {
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
		
			
		Iterable<CSVRecord> allTweets = () -> new IteratorChain<>(trumpTweets.iterator(), bidenTweets.iterator() );

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

			
			System.out.println("ArrayList: " ); 
			System.out.println("");
			//findTopUsingArrayList(allTweets, 20);
			Thread.sleep(1000);
			System.out.println("");
			System.out.println("TreeMap" ); 
			System.out.println(""); 
	        findTopUsingTreeMap(allTweets, 20);
	        Thread.sleep(1000);
	        System.out.println(""); 
	        System.out.println("HashMap" ); 
	        findTopUsingHashMap(allTweets, 20);
	        System.out.println(""); 
	        
	        

		
        
        System.out.println("Time List: " + timeList.getElapsedSeconds()) ;
        System.out.println("Time TreeMap: " + timeTreeMap.getElapsedSeconds()); 
        System.out.println("Time HashMap: " + timeHashMap.getElapsedSeconds());
       
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
		timeList.start(); 
		int numTweets = 0;
		/**
		 * here is the new arraylist and you add each record to the list
		 * First check is to see that there is some data in the screen name field 
		 * 
		 * Options: 
		 * 1: if the list is empty we add to the list 
		 * 2: if the list is not empty
		 * 		2a: check if the screen name is already in the list 
		 * 		2b: if yes then increment else add to the list
		 */
		ArrayList<TweetCount> listOfTweets = new ArrayList<TweetCount>();
		for (CSVRecord record : allTweets) {
			boolean addToList = true;
			if(record.isSet(8)) { // is there something in field 8 
				numTweets++;
				String userName = record.get(8);
				TweetCount tweet = new TweetCount(userName, 1);
				if (listOfTweets.isEmpty()) { // if list is empty add to list 
					listOfTweets.add(tweet);
				} else {
					for (TweetCount tc : listOfTweets) { // check through the list and see if tc is in the list 
						if (tc.getScreenName().equals(userName)) { // if in list increment 
							tc.increment(); 
							addToList = false;
							break; // no need to keep going through the loop we have already found what we are looking for 
						}
					}
					if (addToList == true) { // tc wasn't in list add to list 
						listOfTweets.add(tweet);	
					}
				}		
			}
		}
		
		
		
		PriorityQueue<TweetCount> pQ = pQueue(listOfTweets); 		
		timeList.stop(); 
		stopwatch.stop();
		double time = stopwatch.getElapsedSeconds(); 
		
		/**
		 * print output of the top users by tweet count
		 */
		print (pQ , "an" , "ArrayList", time, numTweets , n) ; 
	 
		
	}
	
	/**
	 * takes a priority queue and some parameters for printing the output 
	 * removes the top n from the priority queue and prints the data 
	 * @param PriorityQueue<TweetCount> pQ 
	 * @param prefix (a , an) 
	 * @param type (ArrayList, TreeMap, HashMap) 
	 * @param time 
	 * @param n
	 */
	public static void print(PriorityQueue<TweetCount> pQ, String prefix, 
			String type, double time, int numTweets ,int n) {
	 
		System.out.printf("To count %,d tweets with %s %s took %f seconds. \n", numTweets ,
				prefix ,  type , time); 
		System.out.printf("The %d users by number of tweets are: \n" , n); 
	
		for(int i = 0 ; i < n ; i ++ ) { // go through the PriorityQueue and remove the n highest priority members and print
			TweetCount temp = pQ.remove(); 
			System.out.println(temp.getScreenName() + " had " + temp.getCount() + " tweets"); 
		}
	}
	
	/**
	 * Accepts a collection and implements them into a PriorityQueue 
	 * @param col
	 * @return returns a Priority Queue 
	 */
	public static PriorityQueue<TweetCount> pQueue(Collection<TweetCount> col) {
		PriorityQueue<TweetCount> pQ = new PriorityQueue<>() ; 
		for (TweetCount tweet : col) {
			pQ.add(tweet); 
		}
		return pQ ; 
	}
	
	
	
	/**
	 * TODO: Add your code for tree maps here. You will do best to create many
	 * methods to organize your work and experiments.
	 * 
	 * Takes iterates through the CSVRecord and puts them in a tree map that is sorted by 
	 * the natural order of the String ScreenName of the TweetCount object using the 
	 * comparable interface in TweetCount 
	 * 
	 * Takes the TreeMap puts it into a PriorityQueue and then prints the top n users by count 
	 * 
	 * @param allTweets the Iterable object that lets you iterate over all of the data
	 * @param n the number of users to report the top N for
	 */
	public static void findTopUsingTreeMap(Iterable<CSVRecord> allTweets, int n) {
		
		timeTreeMap.start() ; 
		
		stopwatch.reset() ; 
		stopwatch.start(); 
		
		//TreeMap sorted by string and mapped to a TweetCount object 
		TreeMap<String, TweetCount> map = new TreeMap<String, TweetCount>() ;
		
		
		int numTweets = 0; 
		/**
		 * Options: 
		 * 	1: if map is empty add to map sorted by screenName 
		 * 	2: if map is not empty 
		 * 		2a: check if map contains userName then increment the Count of the object 
		 * 		2b: if the map doesn't have the TweetCount then then create TweetCount and put it in the map 
		 * 
		 * 
		 */
		for (CSVRecord  record : allTweets) { // for each record add to the TreeMap
			if (record.isSet(8)) {
				String userName = record.get(8); 
				
				if (map.isEmpty()) { // is empty
					TweetCount tweet = new TweetCount(userName , 1); 
					map.put(userName, tweet);
				}
				else {
					TweetCount check = map.get(userName); 
					if (check == null) { 
						TweetCount tweet = new TweetCount(userName, 1); //Create TweetCount
						map.put(userName, tweet);  //add the new TweetCount to the map 
						
					}
					else {
						check.increment(); 
					}
				}
				
				
				
				numTweets++ ; 
			}
			
		}
		
		PriorityQueue<TweetCount> pQueue = pQueue(map.values()) ;  // take the map values and put it into a PriorityQueue
			
			
		timeTreeMap.stop() ;
		double pQTime = stopwatch.getElapsedSeconds() ; 
			
		print(pQueue , "a" , "TreeMap" , pQTime , numTweets , n) ; //Print the PriorityQueue 
			



		
		/**
		 * ******GETTING TOP USERS USING MY ALGORITHM*******
		 * 
		 * This was my idea of how to have a fast leaderboard type setup but i couldn't beat the speed of a priority queue 
		 * it would mathc the speed for long runs with a large leaderboard and lots of data but it had a little too much 
		 * overhead for short runs with a small set of data 
		 * 
		 * the premise : 
		 * it takes a tree map sorted by Strings where each node is a TweetCount object 
		 * Creates a TreeMap sorted by Integer(TweetCount.count()) and each node is a TreeMap<String , TweetCount> 
		 * This way I can have all the TweetCounts with the same count value in the appropriate node 
		 * 
		 * I think there is still a lot of merit to this design even if for this particular application it is not as fast
		 * Also being able to re manipulate a tree map to sort by something else is an important skill to have
		 * 
		 */
		/*
		TreeMap<Integer, TreeMap<String, TweetCount>> leaderBoard = new TreeMap<Integer, TreeMap<String , TweetCount>>() ; 
		
		for (TweetCount key : map.values()) {
			int value = key.getCount() ;
			eets with a HashMap took 0.730283 seconds. 
			if (!leaderBoard.containsKey(value)) {
				TreeMap<String, TweetCount> tree = new TreeMap<String, TweetCount>() ; 
				tree.put(key.getScreenName(), key); 
				leaderBoard.put(key.getCount(), tree); 
				
			}
			else {
				
				leaderBoard.get(key.getCount()).put(key.getScreenName() , key); 
			}
		}
		
		int counter = 0; 
		
		
		for(TreeMap<String , TweetCount> key : leaderBoard.descendingMap().values()) {
			
			for (TweetCount user : key.values()) {
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
		//print(pQueue , "a" , "treeMap" , time, numTweets, n); 
		
		*/

		
	}

	/**
	 * TODO: Add your code for hash maps here. You will do best to create many
	 * methods to organize your work and experiments.
	 * 
	 * 
	 * This method finds the top most active users using a HashMap. 
	 * I was looking for a way to forego the priority queue and use just the HashMap methods 
	 * because HashMaps work in O(1) ish time so it finishes the first part but the second part holds it up 
	 * for relatively a long time but to sort the whole thing only takes about 3 seconds for 1.7million tweets 
	 * and listing the top 400,000 users by TweetCount 
	 * 
	 * @param allTweets the Iterable object that lets you iterate over all of the data
	 * @param n the number of users to report the top N for
	 */
	public static void findTopUsingHashMap(Iterable<CSVRecord> allTweets, int n) {
		/**
		 * Options: 
		 * 	1: if map is empty add to map sorted by screenName 
		 * 	2: if map is not empty 
		 * 		2a: check if map contains userName then increment the Count of the object 
		 * 		2b: if the map doesn't have the TweetCount then then create TweetCount and put it in the map 
		 *
		*/ 

		
		timeHashMap.start() ; 
		stopwatch.reset();
		stopwatch.start() ; 
		int numTweets = 0 ; 
		HashMap<String , TweetCount> map = new HashMap<>() ; 
		for (CSVRecord record : allTweets) {
			if (record.isSet(8)) {
				numTweets ++; 
				String name = record.get(8); 
				if (map.containsKey(name)) {
					map.get(name).increment() ;   
				}
				else {
					TweetCount tweet = new TweetCount (name , 1); 
					map.put(name, tweet); 
				}	
			}
		}
		
		PriorityQueue<TweetCount> pQ = pQueue(map.values()); 
		stopwatch.stop(); 
		double time = stopwatch.getElapsedSeconds() ; 
		timeHashMap.stop() ; 
		print (pQ , "a" ,  "HashMap"  , time , numTweets , n); 
		
		
		
		
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
