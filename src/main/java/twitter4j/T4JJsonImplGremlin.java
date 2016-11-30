package twitter4j;

/**
 * Tweak the visibility of some already existing, useful functionality
 * Created by alsq on 11/19/16.
 */
public class T4JJsonImplGremlin {
    public static Status getStatus(JSONObject json) throws TwitterException {
        return new StatusJSONImpl(json);
    }
}
