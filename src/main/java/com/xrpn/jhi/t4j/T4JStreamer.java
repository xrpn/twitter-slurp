package com.xrpn.jhi.t4j;

import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.PropertyConfiguration;

import java.io.InputStream;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Twitter4j provides a ready-to-use OAuth client for the Twitter implementation of OAuth,
 * as well as a streaming Http client.  The streaming Http client is processed in
 * twitter4j.StatusStreamBase beginning around line 69 (method handleNextLine), providing
 * JSON-parsed record handling, or raw record handling, or both.  The incoming stream is
 * broken into line-records in handleNextLine before concurrent handling for each records.
 * The concurrency of handleNextLine is implemented through java.util.Executors and a
 * fixedThreadPool executor with one thread by default (the number of threads is configurable,
 * say, through async.numThreads in the properties file).  The overhead of raw record handling
 * is modest and the default seems OK to me for this proof-of-concept.  The alternatives are
 * writing from scratch the OAuth client, or reimplementing handleNextLine, or both.
 * <br><br>
 * Twitter.com spritzer (the 1% sampled firehose) has some built-in flow control according to
 * the online docs, to the extent that it can throttle itself for a slow client; but only
 * for a time.  If slowness persists, spritzer will close the stream.  For all practical
 * purposes flow control is not subject to client-controlled back pressure if we want to
 * keep the connection open.  Therefore we have a queuing model where the arrival interval
 * is arbitrary (likely small), but we can control the buffer size and the service time.
 * Little's law determines the size of the buffer given the average arrival interval and
 * the average service time.  In real life the alchemy of establishing service time is
 * complex.  The buffer, if local memory is not enough, could be perhaps something like
 * Redis (http://redis.io/) or memcached (https://memcached.org/).  For exceptional
 * capacity requirements (e.g. during downtime) maybe kafka (https://kafka.apache.org/)
 * could do.
 * <br><br>
 * Created by alsq on 11/18/16.
 */
public class T4JStreamer {

    // configuration priming
    private static String propsFn = "twitter4j.properties";
    private static InputStream props = T4JStreamer.class.getClassLoader().getResourceAsStream(propsFn);

    public static final Configuration config = new PropertyConfiguration(props);
    public static final ConcurrentLinkedQueue<String> rawBuffer = new ConcurrentLinkedQueue<>();

    /**
     * My prying eye into what a JSON-parsed stream looks like.
     */
    public static void trivialStatusSampler() {
        TwitterStream twitterStream = new TwitterStreamFactory(config).getInstance();
        twitterStream.addListener(T4JListener.trivialStatusListener);
        twitterStream.sample();
        try { Thread.sleep(2000);} catch ( InterruptedException ignored ) {};
        twitterStream.cleanUp();
        twitterStream.shutdown();
    }

    /**
     * My prying eye into what a raw stream looks like.
     */
    public static void trivialRawSampler() {
        TwitterStream twitterStream = new TwitterStreamFactory(config).getInstance();
        twitterStream.addListener(T4JListener.trivialRawListener);
        twitterStream.sample();
        try { Thread.sleep(4000);} catch ( InterruptedException ignored ) {};
        twitterStream.cleanUp();
        twitterStream.shutdown();
    }

    /**
     * Receive raw samples into a buffer.  Stop after a deterministic sampling period.
     * @param msSample duration in ms of the sample period
     */
    public static void bufferingRawSampler(long msSample) {
        TwitterStream twitterStream = new TwitterStreamFactory(config).getInstance();
        twitterStream.addListener(T4JListener.bufferingRawListener);
        twitterStream.sample();
        try { Thread.sleep(msSample);} catch ( InterruptedException ignored ) {};
        twitterStream.cleanUp();
        twitterStream.shutdown();
    }

    public static TwitterStream bufferingRawSampler() {
        TwitterStream twitterStream = new TwitterStreamFactory(config).getInstance();
        twitterStream.addListener(T4JListener.bufferingRawListener);
        twitterStream.sample();
        return twitterStream;
    }

    /**
     * Receive raw samples into a buffer, with arrival stats.  Stop after a deterministic sampling period.
     * @param stateful the listener with stats housekeeping capability
     * @param msSample duration in ms of the sample period
     */
    public static void instrumentedBufferingRawSampler(T4JInstrumentedListener stateful, long msSample) {
        TwitterStream twitterStream = new TwitterStreamFactory(config).getInstance();
        twitterStream.addListener(stateful.instrumentedBufferingRawListener);
        twitterStream.sample();
        try { Thread.sleep(msSample);} catch ( InterruptedException ignored ) {};
        twitterStream.cleanUp();
        twitterStream.shutdown();
    }

    /**
     * Receive raw samples into a buffer, with arrival stats.  Continue until the stream
     * is stopped in some manner to be defined by the caller.  If not stopped, sampling
     * will continue forever.
     * @param stateful the listener with stats housekeeping capability
     */
    public static TwitterStream instrumentedBufferingRawSampler(T4JInstrumentedListener stateful) {
        TwitterStream twitterStream = new TwitterStreamFactory(config).getInstance();
        twitterStream.addListener(stateful.instrumentedBufferingRawListener);
        twitterStream.sample();
        return twitterStream;
    }
}
