package org.greg.server;

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.TDistributionImpl;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;

public class GregServer {
    private static String get(String[] args, String arg, String def) {
        for (int i = 0; i < args.length - 1; ++i) {
            if (args[i].equals("-" + arg))
                return args[i + 1];
        }
        return def;
    }

    private static int get(String[] args, String arg, int def) {
        return Integer.parseInt(get(args, arg, "" + def));
    }

    private static double get(String[] args, String arg, double def) {
        return Double.parseDouble(get(args, arg, "" + def));
    }


    public static void main(String[] args) {
        Trace.writeLine("GregServer started");

        Configuration conf = new Configuration();
        conf.messagePort = get(args, "port", 5676);
        conf.calibrationPort = get(args, "calibrationPort", 5677);
        conf.desiredConfidenceLevel = get(args, "confidenceLevel", 0.95);
        conf.desiredConfidenceRangeMs = get(args, "confidenceRangeMs", 1);
        conf.maxCalibrationIters = get(args, "maxCalibrationIters", 100);
        conf.minCalibrationIters = get(args, "minCalibrationIters", 10);
        conf.preCalibrationIters = get(args, "preCalibrationIters", 10);
        conf.maxPendingCalibrated = get(args, "maxPendingCalibrated", 1000000);
        conf.maxPendingUncalibrated = get(args, "maxPendingUncalibrated", 100000);
        conf.timeWindowSec = get(args, "timeWindowSec", 5);

        if(Boolean.parseBoolean(get(args, "verbose", "false"))) {
            Trace.ENABLED = true;
        }

        final GregServer server = new GregServer(conf);

        new Thread() {
            public void run() {
                server.acceptMessages();
            }
        }.start();
        new Thread() {
            public void run() {
                server.acceptCalibration();
            }
        }.start();
        new Thread() {
            public void run() {
                server.flushCalibratedMessages();
            }
        }.start();

        new Thread() {
            public void run() {
                try {
                    Thread.currentThread().setPriority(7); // Above normal

                    Writer writer = new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(FileDescriptor.out), 16384));

                    String newline = System.getProperty("line.separator");

                    while (true) {
                        List<Record> records = server.outputQueue.dequeue();
                        if (records.isEmpty()) {
                            Thread.sleep(50);
                            continue;
                        }
                        for (Record rec : records) {
                            writer.write(rec.machine);
                            writer.write(' ');
                            writer.write(rec.clientId);
                            writer.write(' ');
                            writer.write(rec.timestamp.toString());
                            writer.write(' ');
                            writer.write(rec.message);
                            writer.write(newline);
                        }
                        writer.flush();
                    }
                } catch (Exception e) {
                    Trace.writeLine("Failure while writing records", e);
                }
            }
        }.start();
    }

    private final Configuration conf;
    private final TimeBufferedQueue<Record> outputQueue;

    private final ConcurrentMap<UUID, Queue<Record>> clientRecords = new ConcurrentHashMap<UUID, Queue<Record>>();
    private final ConcurrentMap<UUID, TimeSpan> clientLateness = new ConcurrentHashMap<UUID, TimeSpan>();
    private final AtomicInteger numPendingUncalibratedEntries = new AtomicInteger(0);

    public GregServer(Configuration conf) {
        this.conf = conf;

        Comparator<Record> RECORD_COMPARATOR = new Comparator<Record>() {
            public int compare(Record x, Record y) {
                return x.timestamp.compareTo(y.timestamp);
            }
        };

        this.outputQueue = new TimeBufferedQueue<Record>(
                new TimeSpan(conf.timeWindowSec * 1000000000L),
                PreciseClock.INSTANCE,
                conf.maxPendingCalibrated,
                RECORD_COMPARATOR);
    }


    private void acceptMessages() {
        ServerSocket server = null;
        try {
            server = new ServerSocket(conf.messagePort);
        } catch (IOException e) {
            Trace.writeLine("Cannot create messages acceptor", e);
        }

        int maxConcurrentClients = 16;
        final Semaphore sem = new Semaphore(maxConcurrentClients);

        Executor pool = Executors.newFixedThreadPool(16);

        while (true) {
            try {
                sem.acquire();
            } catch (InterruptedException e) {
                continue;
            }

            final Socket client;
            final InputStream stream;
            try {
                client = server.accept();
                stream = client.getInputStream();
            } catch (Exception e) {
                Trace.writeLine("Failed to accept client or get its input stream", e);
                continue;
            }

            pool.execute(new Runnable() {
                public void run() {
                    try {
                        processRecordsBatch(stream, client.getRemoteSocketAddress());
                    } finally {
                        sem.release();
                        try {
                            client.close();
                        } catch (Exception e) {
                            // Ignore
                        }
                    }
                }
            });
        }
    }

    private void processRecordsBatch(InputStream rawStream, final SocketAddress ep) {
        try {
            InputStream stream = new BufferedInputStream(rawStream, 65536);

            DataInput r = new LittleEndianDataInputStream(stream);
            UUID uuid = new UUID(r.readLong(), r.readLong());
            boolean useCompression = r.readBoolean();

            clientRecords.putIfAbsent(uuid, new ConcurrentLinkedQueue<Record>());
            final Queue<Record> q = clientRecords.get(uuid);

            final AtomicBoolean skipping = new AtomicBoolean(false);
            final AtomicInteger numRead = new AtomicInteger(0);
            final AtomicInteger numSkipped = new AtomicInteger(0);
            Sink<Record> sink = new Sink<Record>() {
                public void consume(Record rec) {
                    numRead.incrementAndGet();
                    int numPending = numPendingUncalibratedEntries.incrementAndGet();
                    if (numPending < conf.maxPendingUncalibrated) {
                        q.offer(rec);

                        if (skipping.get()) {
                            Trace.writeLine("Receiving entries from client " + ep + " again, after having skipped " + numSkipped);
                        }
                        skipping.set(false);
                        numSkipped.set(0);
                    } else {
                        numPendingUncalibratedEntries.decrementAndGet();

                        numSkipped.incrementAndGet();
                        if (!skipping.get() || numSkipped.get() % 10000 == 0) {
                            Trace.writeLine(
                                    "Uncalibrated records buffer full - skipping entry from client " + ep +
                                            " because there are already " + numPending + " uncalibrated entries. " +
                                            (numSkipped.get() == 1 ? "" : (numSkipped + " skipped in a row...")));
                        }
                        skipping.set(true);
                    }
                }
            };
            readRecords(useCompression ? new GZIPInputStream(stream) : stream, sink);

            if (skipping.get()) {
                Trace.writeLine("Skipped " + numSkipped + " entries from " + ep + " in a row.");
            }
            Trace.writeLine("Read " + numRead + " entries");
        } catch (Exception e) {// Socket or IO or whatever
            Trace.writeLine("Failed to receive records batch, ignoring", e);
            // Ignore
        }
    }

    private interface Sink<T> {
        void consume(T t);
    }

    private static void readRecords(InputStream stream, Sink<Record> sink) throws IOException {
        DataInput r = new LittleEndianDataInputStream(new BufferedInputStream(stream, 65536));

        int cidLenBytes = r.readInt();
        byte[] cidBytes = new byte[cidLenBytes];
        r.readFully(cidBytes);
        String clientId = new String(cidBytes, "utf-8");

        while (0 != r.readInt()) {
            PreciseDateTime timestamp = new PreciseDateTime(r.readLong());
            int machineLenBytes = r.readInt();
            byte[] machineBytes = new byte[machineLenBytes];
            r.readFully(machineBytes);
            String machine = new String(machineBytes, "utf-8");
            int msgLenBytes = r.readInt();
            byte[] msgBytes = new byte[msgLenBytes];
            r.readFully(msgBytes);
            String msg = new String(msgBytes, "utf-8");

            Record rec = new Record();
            rec.machine = machine;
            rec.timestamp = timestamp;
            rec.message = msg;
            rec.serverTimestamp = PreciseClock.INSTANCE.now();
            rec.clientId = clientId;
            sink.consume(rec);
        }
    }

    private void acceptCalibration() {
        ServerSocket calibrationServer = null;
        try {
            calibrationServer = new ServerSocket(conf.calibrationPort);
        } catch (IOException e) {
            Trace.writeLine("Failed to create calibration listener", e);
        }

        Executor executor = Executors.newFixedThreadPool(16);

        while (true) {
            final Socket client;
            try {
                client = calibrationServer.accept();
            } catch (Exception e) {
                Trace.writeLine("Failed to accept client for calibration or get its endpoint", e);
                continue;
            }
            final SocketAddress ep = client.getRemoteSocketAddress();

            executor.execute(new Runnable() {
                public void run() {
                    try {
                        processCalibrationExchange(client, ep);
                    } catch (Exception e) {
                        Trace.writeLine("Failed to process calibration exchange with " + ep, e);
                    } finally {
                        try {
                            client.close();
                        } catch (IOException e) {
                            // Ignore
                        }
                    }
                }
            });
        }
    }

    private void processCalibrationExchange(Socket client, SocketAddress ep) throws IOException {
        InputStream in = client.getInputStream();
        OutputStream out = client.getOutputStream();
        // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#On-line_algorithm

        DataOutput w = new LittleEndianDataOutputStream(out);
        DataInput r = new LittleEndianDataInputStream(in);

        UUID uuid = new UUID(r.readLong(), r.readLong());

        // We exchange *ticks* (0.1us intervals)

        // Do some iterations to warm up the TCP connection
        for (int i = 0; i < conf.preCalibrationIters; ++i) {
            w.writeLong(PreciseClock.INSTANCE.now().toUtcNanos());
            r.readLong();
        }

        long mean = 0;
        long m2 = 0;

        try {
            for (int i = 0; i < conf.maxCalibrationIters; ++i) {
                PreciseDateTime beforeSend = PreciseClock.INSTANCE.now();
                w.writeLong(beforeSend.toUtcNanos());
                PreciseDateTime clientTime = new PreciseDateTime(r.readLong());
                PreciseDateTime afterReceive = PreciseClock.INSTANCE.now();

                long latencyNanos = (afterReceive.toUtcNanos() - beforeSend.toUtcNanos()) / 2;
                long clockLatenessNanos = clientTime.toUtcNanos() - beforeSend.toUtcNanos() - latencyNanos;
                // clientTime   == beforeSend + clockLateness + latency
                // afterReceive == clientTime - clockLateness + latency
//                Trace.writeLine(
//                    "[" + ep + "] Iteration " + i + ": clock late by " +
//                    new TimeSpan(clockLateness) +
//                    " (beforeSend: " + beforeSend.toString() +
//                    ", clientTime: " + clientTime.toString() +
//                    ", afterReceive: " + afterReceive.toString()+ ")");

                int n = i + 1;

                long delta = clockLatenessNanos - mean;
                mean += delta / n;
                m2 += delta * (clockLatenessNanos - mean);

                if (n == 1) continue;

                double s = Math.sqrt(1.0 * m2 / (n - 1));

                double td = new TDistributionImpl(n - 1).cumulativeProbability((1 + conf.desiredConfidenceLevel) / 2);
                double confidenceRange = 2 * s / Math.sqrt(n) * td;
//                Trace.writeLine("[" + ep + "] confidence range = " + confidenceRange / 1000000 + " ms");
                if (i >= conf.minCalibrationIters && confidenceRange < conf.desiredConfidenceRangeMs * 1000000) {
//                    Trace.writeLine("[" + ep + "] Achieved desired confidence range");
                    break;
                }
            }
        } catch (MathException e) {
            Trace.writeLine("Math exception while calibrating with " + ep, e);
        }

        Trace.writeLine("Clock lateness with client " + ep + " (" + uuid + ") is " + new TimeSpan(mean));
        clientLateness.put(uuid, new TimeSpan(mean));
    }


    private void flushCalibratedMessages() {
        Thread.currentThread().setPriority(7); // above normal
        while(true) {
            for (Map.Entry<UUID, TimeSpan> p : clientLateness.entrySet()) {
                UUID client = p.getKey();
                TimeSpan lateness = p.getValue();

                Queue<Record> q = clientRecords.get(client);
                if(q != null) {
                    List<Pair<PreciseDateTime, Record>> snapshot = new ArrayList<Pair<PreciseDateTime, Record>>();
                    for(int i = 0; i < 10000; ++i) {
                        Record r = q.poll();
                        if(r == null) {
                            break;
                        }
                        snapshot.add(absolutizeTime(r, lateness));
                    }
//                    Trace.writeLine("Dequeued snapshot: " + snapshot.size());
                    numPendingUncalibratedEntries.addAndGet(-snapshot.size());
                    outputQueue.enqueue(snapshot);
                }
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                continue;
            }
        }
    }

    private static Pair<PreciseDateTime, Record> absolutizeTime(Record rec, TimeSpan lateness) {
        PreciseDateTime t = new PreciseDateTime(rec.timestamp.toUtcNanos() - lateness.toNanos());

        Record r = new Record();
        r.clientId = rec.clientId;
        r.machine = rec.machine;
        r.message = rec.message;
        r.timestamp = t;
        r.serverTimestamp = rec.serverTimestamp;

        return new Pair<PreciseDateTime, Record>(t, r);
    }
}
