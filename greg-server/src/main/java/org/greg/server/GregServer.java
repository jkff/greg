package org.greg.server;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.*;
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

                    OutputStream os = new BufferedOutputStream(new FileOutputStream(FileDescriptor.out), 16384);

                    byte[] newline = System.getProperty("line.separator").getBytes("utf-8");

                    while (true) {
                        List<Record> records = server.outputQueue.dequeue();
                        if (records.isEmpty()) {
                            Thread.sleep(50);
                            continue;
                        }

                        for (Record rec : records) {
                            os.write(rec.machine.array, rec.machine.offset, rec.machine.len);
                            os.write(' ');
                            os.write(rec.clientId.array, rec.clientId.offset, rec.clientId.len);
                            os.write(' ');
                            byte[] ts = rec.timestamp.toBytes();
                            os.write(ts);
                            os.write(' ');
                            os.write(rec.message.array, rec.message.offset, rec.message.len);
                            os.write(newline);
                        }
                        os.flush();
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
            final UUID uuid = new UUID(r.readLong(), r.readLong());
            boolean useCompression = r.readBoolean();

            clientRecords.putIfAbsent(uuid, new ConcurrentLinkedQueue<Record>());
            final Queue<Record> q = clientRecords.get(uuid);

            final boolean[] skipping = new boolean[] {false};
            final int[] numRead = {0};
            final int[] numSkipped = {0};

            final List<Record> uncalibrated = new ArrayList<Record>();
            final List<Pair<PreciseDateTime, Record>> calibrated = new ArrayList<Pair<PreciseDateTime, Record>>();

            final TimeSpan lateness = clientLateness.get(uuid);

            Sink<Record> sink = new Sink<Record>() {
                public void consume(Record rec) {
                    numRead[0]++;

                    if(lateness == null) {
                        int numPending = numPendingUncalibratedEntries.incrementAndGet();
                        if (numPending < conf.maxPendingUncalibrated) {
                            uncalibrated.add(rec);

                            if (skipping[0]) {
                                Trace.writeLine("Receiving entries from client " + ep + " again, after having skipped " + numSkipped[0]);
                            }
                            skipping[0] = false;
                            numSkipped[0] = 0;
                        } else {
                            numPendingUncalibratedEntries.decrementAndGet();

                            numSkipped[0]++;
                            if (!skipping[0] || numSkipped[0] % 10000 == 0) {
                                Trace.writeLine(
                                        "Uncalibrated records buffer full - skipping entry from client " + ep +
                                                " because there are already " + numPending + " uncalibrated entries. " +
                                                (numSkipped[0] == 1 ? "" : (numSkipped[0] + " skipped in a row...")));
                            }
                            skipping[0] = true;
                        }
                    } else {
                        calibrated.add(absolutizeTime(rec, lateness));
                    }
                }
            };

            readRecords(useCompression ? new GZIPInputStream(stream) : stream, sink);

            // Only publish records to main queue if we read all them successfully (had no exception to this point)
            // Otherwise we'd have duplicates if clients resubmit their records after failure.
            q.addAll(uncalibrated);
            outputQueue.enqueue(calibrated);

            if (skipping[0]) {
                Trace.writeLine("Skipped " + numSkipped[0] + " entries from " + ep + " in a row.");
            }
            Trace.writeLine("Read " + numRead[0] + " entries");
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

        ByteSlice clientId = new ByteSlice(cidBytes, 0, cidBytes.length);

        while (0 != r.readInt()) {
            PreciseDateTime timestamp = new PreciseDateTime(r.readLong());
            int machineLenBytes = r.readInt();
            byte[] machineBytes = new byte[machineLenBytes];
            r.readFully(machineBytes);
            int msgLenBytes = r.readInt();
            byte[] msgBytes = new byte[msgLenBytes];
            r.readFully(msgBytes);

            Record rec = new Record();
            rec.machine = new ByteSlice(machineBytes, 0, machineLenBytes);
            rec.timestamp = timestamp;
            rec.message = new ByteSlice(msgBytes, 0, msgBytes.length);
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
                client.setTcpNoDelay(true);
            } catch (Exception e) {
                Trace.writeLine("Failed to accept client for calibration", e);
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

        DataOutput w = new LittleEndianDataOutputStream(out);
        DataInput r = new LittleEndianDataInputStream(in);

        UUID uuid = new UUID(r.readLong(), r.readLong());

        // We exchange *ticks* (0.1us intervals)

        // Do some iterations to warm up the TCP connection
        for (int i = 0; i < conf.preCalibrationIters; ++i) {
            w.writeLong(PreciseClock.INSTANCE.now().toUtcNanos());
            r.readLong();
        }

        // The smaller the network roundtrip, the smaller the range of possible
        // asymmetries of network latencies, the more precisely we compute the
        // clock difference.
        long minLatencyNanos = Long.MAX_VALUE;
        long latenessAtMinLatencyNanos = 0;

        for (int i = 0; i < conf.maxCalibrationIters; ++i) {
            PreciseDateTime beforeSend = PreciseClock.INSTANCE.now();
            w.writeLong(beforeSend.toUtcNanos());
            PreciseDateTime clientTime = new PreciseDateTime(r.readLong());
            PreciseDateTime afterReceive = PreciseClock.INSTANCE.now();

            long latencyNanos = (afterReceive.toUtcNanos() - beforeSend.toUtcNanos()) / 2;
            if(latencyNanos < minLatencyNanos) {
                minLatencyNanos = latencyNanos;
                latenessAtMinLatencyNanos = clientTime.toUtcNanos() - beforeSend.toUtcNanos() - latencyNanos;
            }

            if (i >= conf.minCalibrationIters && latencyNanos < conf.desiredConfidenceRangeMs * 1000000L) {
                break;
            }
        }

        TimeSpan lateness = new TimeSpan(latenessAtMinLatencyNanos);
        Trace.writeLine("Clock lateness with client " + ep + " (" + uuid + ") is " + lateness);
        clientLateness.put(uuid, lateness);
    }


    private void flushCalibratedMessages() {
        Thread.currentThread().setPriority(7); // above normal
        while(true) {
            List<Pair<PreciseDateTime, Record>> snapshot = new ArrayList<Pair<PreciseDateTime, Record>>(10000);
            for (Map.Entry<UUID, TimeSpan> p : clientLateness.entrySet()) {
                UUID client = p.getKey();
                TimeSpan lateness = p.getValue();

                Queue<Record> q = clientRecords.get(client);
                if(q != null) {
                    snapshot.clear();
                    for(int i = 0; i < 10000; ++i) {
                        Record r = q.poll();
                        if(r == null) {
                            break;
                        }
                        snapshot.add(absolutizeTime(r, lateness));
                    }
                    Trace.writeLine("Dequeued snapshot: " + snapshot.size());
                    numPendingUncalibratedEntries.addAndGet(-snapshot.size());
                    outputQueue.enqueue(snapshot);
                }
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                continue;
            }
        }
    }

    private static Pair<PreciseDateTime, Record> absolutizeTime(Record rec, TimeSpan lateness) {
        PreciseDateTime t = new PreciseDateTime(rec.timestamp.toUtcNanos() - lateness.toNanos());

        rec.timestamp = t;

        return new Pair<PreciseDateTime, Record>(t, rec);
    }
}
