package com.xrpn.jhi.t4j;

import org.slf4j.Logger;
import twitter4j.RawStreamListener;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.xrpn.jhi.t4j.T4JStreamer.rawBuffer;

/**
 * A listener that keeps track of number of arrivals, and of average time lapse
 * (in microseconds, if the platform can do that) between arrivals.  This
 * housekeeping takes a little precious time, but can be valuable in choosing
 * a buffer size.
 *
 * Created by alsq on 11/18/16.
 */
public class T4JInstrumentedListener {

    Logger log = org.slf4j.LoggerFactory.getLogger(getClass());

    /**
     * Time between arrivals in microseconds.  This is a running average.
     */
    // currentTimeMillis() flops during bursts, whence the microsecond tally
    final AtomicInteger usAverageArrivalInterval = new AtomicInteger(0);
    /**
     * Running tally of number of arrivals.
     */
    final AtomicInteger numberOfArrivals = new AtomicInteger(0);
    // TODO: need find a better way to initialize lastArrival
    // there is a lapse in establishing the connection and beginning
    // sampling that introduces a systematic bias.  It's negligible
    // for large n but annoying for small ones, e.g. when testing
    private final AtomicLong lastArrival = new AtomicLong(System.nanoTime());

    /**
     * Send raw records to a buffer, and update arrival stats.
     */
    public RawStreamListener instrumentedBufferingRawListener = new RawStreamListener() {
        public void onMessage(String rawString)  {
            long thisArrival = System.nanoTime();
            // TODO: this is not strictly accurate because of possible race conditions
            // In case the listener is called from multiple threads there may be race
            // conditions among the counters.  This will be possible if and only if
            // async.numThreads is larger than 1 in the configuration (by default there
            // is only one thread).  For large n and ordinary enough arrival intervals
            // the error could be small even if multiple threads are running.
            // Since we know nothing about the distribution of the arrival times, e.g.
            // how big (or not) are the tails, and the default is one thread, there is
            // no need to split hair now with a more complex synchronization scheme.
            double delta = thisArrival - lastArrival.get();
            lastArrival.set(thisArrival);
            /* in microseconds */ delta = delta / 1000.0;
            int ni = numberOfArrivals.getAndIncrement();
            double n = ni;
            double avg = usAverageArrivalInterval.get();
            double tot = n*avg;
            if ((tot+delta)==tot) throw new RuntimeException("numerical precision overflow");
            int newAvg = new Double((tot+delta)/(n+1.0)).intValue();
            usAverageArrivalInterval.set(newAvg);
            if (0 == ni % 1000) log.info("at "+ni+" arrivals, mean arrival interval in ms is "+(newAvg/1000.0));
            rawBuffer.add(rawString);
        };
        public void onException(Exception ex) { ex.printStackTrace(); }
    };

}
