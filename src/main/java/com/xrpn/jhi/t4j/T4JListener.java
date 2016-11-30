package com.xrpn.jhi.t4j;

import twitter4j.*;

import static com.xrpn.jhi.t4j.T4JStreamer.rawBuffer;

/**
 * Twitter4j custom listeners.
 * Created by alsq on 11/18/16.
 */
public class T4JListener {

    /**
     * Print the tweet portion of a JSON-parsed record to stdout.
     */
    public static StatusListener trivialStatusListener = new StatusListener() {
        public void onStatus(Status status) {
            System.out.println(status.getText());
        }
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
        public void onException(Exception ex) {
            ex.printStackTrace();
        }
        public void onScrubGeo(long arg0, long arg1)  {}
        public void onStallWarning(StallWarning warning) {}
    };

    /**
     * Print a raw record to stdout.
     */
    public static RawStreamListener trivialRawListener = new RawStreamListener() {
        public void onMessage(String rawString)  {
            System.out.println(rawString);
        }
        public void onException(Exception ex) { ex.printStackTrace(); }
    };

    /**
     * Send raw records to a buffer.
     */
    public static RawStreamListener bufferingRawListener = new RawStreamListener() {
        public void onMessage(String rawString)  { rawBuffer.add(rawString); };
        public void onException(Exception ex) { ex.printStackTrace(); }
    };

}
