package org.greg.server;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TimeBufferedQueue<T> {
    private final TimeSpan windowSize;
    private final Clock clock;

    private final PriorityQueue<Pair<PreciseDateTime, T>> inputQueue;
    private final int maxQueuedRecords;
    private int numRecords = 0;

    private final Object sync = new Object();

    public TimeBufferedQueue(TimeSpan windowSize, Clock clock, int maxQueuedRecords, Comparator<T> comparer) {
        this.windowSize = windowSize;
        this.clock = clock;
        this.maxQueuedRecords = maxQueuedRecords;
        this.inputQueue = new PriorityQueue<Pair<PreciseDateTime, T>>(1, Pair.<PreciseDateTime, T>compareOnSecond(comparer));
    }

    public void enqueue(List<Pair<PreciseDateTime,T>> entries) {
        synchronized(sync) {
            int toOffer = Math.min(entries.size(), maxQueuedRecords - numRecords);

            for (int i = 0; i < toOffer; ++i) {
                inputQueue.offer(entries.get(i));
            }

            numRecords += toOffer;
            if(entries.size() > toOffer) {
                Trace.writeLine("Calibrated records buffer full - " + (entries.size() - toOffer) + " records dropped");
            }
        }
    }

    public List<T> dequeue() {
        List<T> res = new ArrayList<T>();
        PreciseDateTime dueTime = clock.now();

        synchronized (sync) {
            while (true) {
                Pair<PreciseDateTime,T> item = inputQueue.peek();
                if (item == null || item.first.add(windowSize).compareTo(dueTime) > 0)
                    break;
                inputQueue.remove();
                numRecords--;
                res.add(item.second);
            }
        }

        return res;
    }
}
