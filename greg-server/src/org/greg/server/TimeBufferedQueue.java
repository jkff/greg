package org.greg.server;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class TimeBufferedQueue<T> {
    private final TimeSpan windowSize;
    private final Clock clock;

    private final PriorityQueue<Pair<PreciseDateTime, T>> inputQueue;
    private final int maxQueuedRecords;
    private final AtomicInteger numRecords = new AtomicInteger(0);

    private final Object sync = new Object();

    public TimeBufferedQueue(TimeSpan windowSize, Clock clock, int maxQueuedRecords, Comparator<T> comparer) {
        this.windowSize = windowSize;
        this.clock = clock;
        this.maxQueuedRecords = maxQueuedRecords;
        this.inputQueue = new PriorityQueue<Pair<PreciseDateTime, T>>(1, Pair.<PreciseDateTime, T>compareOnSecond(comparer));
    }

    public void enqueue(Iterable<Pair<PreciseDateTime,T>> entries) {
        synchronized(sync) {
            boolean dropping = false;
            int numDropped = 0;

            for (Pair<PreciseDateTime, T> pair : entries) {
                if (!dropping && numRecords.incrementAndGet() > maxQueuedRecords) {
                    numRecords.decrementAndGet();
                    dropping = true;
                }

                if (dropping) {
                    numDropped++;
                } else {
                    inputQueue.offer(pair);
                }
            }

            if (dropping) {
                Trace.writeLine("Calibrated records buffer full - " + numDropped + " records dropped");
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
                numRecords.decrementAndGet();
                res.add(item.second);
            }
        }

        return res;
    }
}
