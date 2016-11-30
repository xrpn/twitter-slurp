package com.xrpn.jhi.t4j;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Created by alsq on 11/18/16.
 */
public class T4JStreamerTest {

    @Before
    public void setUp() {
        T4JStreamer.rawBuffer.clear();
    }

    @Ignore
    @Test
    public void testTrivialStatusSampler() throws Exception {
        T4JStreamer.trivialStatusSampler();
    }

    @Test
    public void testTrivialRawSampler() throws Exception {
        T4JStreamer.trivialRawSampler();
    }

    @Ignore
    @Test
    public void testBufferingRawSampler() throws Exception {
        T4JStreamer.bufferingRawSampler(4000);
        T4JStreamer.rawBuffer.forEach(s -> System.out.println(s));
    }

    @Ignore
    @Test
    public void testInstrumentedBufferingRawSampler() throws Exception {
        long msSample = 8000;
        T4JInstrumentedListener stateful = new T4JInstrumentedListener();
        // there is a lapse in establishing the connection and beginning sampling
        T4JStreamer.instrumentedBufferingRawSampler(stateful,msSample);
        int size = T4JStreamer.rawBuffer.size();
        int arrivals = stateful.numberOfArrivals.intValue();
        int avgInterval = stateful.usAverageArrivalInterval.intValue();
        System.out.println("size: "+size+", arrivals: "+arrivals+", interval (us): "+avgInterval);
        assertTrue(size==arrivals);
        // allow some tolerance for the setup lapse error
        assertTrue(arrivals*avgInterval < msSample*1100);
    }
}