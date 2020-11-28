import java.util.Objects;

/**
 * This object represents the count of tweets by a specific
 * Twitter user.
 * 
 * @author Mark Hancock
 *
 */
public class TweetCount implements Comparable<TweetCount> {

    /**
     * The screen name of the Twitter user.
     */
    private String screenName;

    /**
     * The count of tweets by the user.
     */
    private int count;

    /**
     * Creates a new TweetCount object with the given screen
     * name and count.
     * 
     * @param screenName the screen name of the user
     * @param count      the initial count of tweets by that
     *                   user.
     */
    public TweetCount(String screenName, int count) {
        this.screenName = screenName;
        this.count = count;
    }

    /**
     * Returns the screen name of the Twitter user.
     * 
     * @return the screen name of the Twitter user.
     */
    public String getScreenName() {
        return screenName;
    }

    /**
     * Returns the count of tweets by this user.
     * 
     * @return the count of tweets by this user.
     */
    public int getCount() {
        return count;
    }

    /**
     * Increments the count of tweets by this user.
     */
    public void increment() {
        count++;
    }

    /**
     * Checks for equality of this TweetCount with another.
     */
    @Override
    public boolean equals(Object other) {
        // Source:
        // https://www.sitepoint.com/implement-javas-equals-method-correctly/
        // self check
        if (this == other)
            return true;

        // null check
        if (other == null)
            return false;

        // type check and cast
        if (getClass() != other.getClass())
            return false;

        TweetCount count = (TweetCount) other;

        // field comparison (only check degree for equality)
        return Objects.equals(screenName, count.screenName);
    }
    
    /**
     * 
     * @param other
     * @return negative if the other has more tweets, 0 if equal, positive if this has more
     */
    @Override 
    public int compareTo(TweetCount other) {
		return  other.getCount() - this.getCount() ; 
		
	}

    /**
     * Ensures that using a HashMap or HashSet on TweetCount
     * objects uses the screen name.
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(screenName);
    }
}
