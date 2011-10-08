package org.greg.client;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

public class Greg {
    private static final ConcurrentLinkedQueue<Record> records = new ConcurrentLinkedQueue<Record>();
    private static final AtomicInteger numDropped = new AtomicInteger(0);
    // Don't use ConcurrentLinkedQueue.size() because it's O(n)
    private static final AtomicInteger numRecords = new AtomicInteger(0);
    private static final Configuration conf = Configuration.INSTANCE;

    private static final UUID OUR_UUID = UUID.randomUUID();
    private static final String hostname;

    private static Thread pushMessagesThread;
    private static Thread initCalibrationThread;

    private static volatile boolean isSoftShutdownRequested = false;
    private static volatile boolean isHardShutdownRequested = false;

    static {
        pushMessagesThread = new Thread("GregPushMessages") {
            public void run() {
                pushCurrentMessages();
            }
        };
        pushMessagesThread.setDaemon(true);
        pushMessagesThread.start();
        initCalibrationThread = new Thread("GregInitiateCalibration") {
            public void run() {
                initiateCalibration();
            }
        };
        initCalibrationThread.setDaemon(true);
        initCalibrationThread.start();

        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new AssertionError("Can't get localhost?");
        }
    }

    public static void log(String message) {
        if (numRecords.get() < conf.maxBufferedRecords) {
            numRecords.incrementAndGet();

            Record r = new Record();
            r.message = message;
            r.timestamp = PreciseClock.INSTANCE.now();
            int prevNumDropped = numDropped.getAndSet(0);
            if (prevNumDropped > 0) {
                Trace.writeLine("Stopped dropping messages, " + prevNumDropped + " were dropped");
            }
            records.offer(r);
        } else {
            int newNumDropped = numDropped.incrementAndGet();
            if (newNumDropped == 0) {
                Trace.writeLine("Starting to drop messages because of full queue");
            } else if (newNumDropped % 100000 == 0) {
                Trace.writeLine(newNumDropped + " dropped in a row...");
            }
        }
    }

    /**
     * Requests shutdown and awaits until either
     * 1) remaining message count reaches zero or
     * 2) timeout elapses.
     * Use this method to make sure that everything your application had to say
     * has been sent, before shutting down the application.
     *
     * Completion of this method DOES NOT prevent further usage of {@link #log(String)}.
     *
     * @param timeoutMs How long to wait before abandoning hope to push all messages
     * (if this much elapses, all bets are off as to what messages have been pushed)
     * @return approximate number of message that haven't been pushed
     * @throws InterruptedException if the thread running this method is interrupted
     */
    public static int shutdownAndAwait(long timeoutMs) throws InterruptedException {
        isSoftShutdownRequested = true;

        long t0 = System.currentTimeMillis();
        pushMessagesThread.join(timeoutMs);
        long elapsed = System.currentTimeMillis() - t0;
        if(elapsed < timeoutMs)
            initCalibrationThread.join(timeoutMs - elapsed);

        isHardShutdownRequested = true;
        // Now the two threads should shut down soon.
        // However we don't care much, as they're daemon threads anyway.

        return records.size();
    }

    private static boolean shouldTerminate() {
        return isHardShutdownRequested || (isSoftShutdownRequested && records.isEmpty());
    }

    private static void pushCurrentMessages() {
        while (true) {
            if (shouldTerminate())
                break;

            while (records.isEmpty()) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    return;
                }
            }
            boolean exhausted = true;
            Socket client = null;
            OutputStream bStream = null;
            OutputStream stream = null;

            try {
                client = new Socket(conf.server, conf.port);
                Trace.writeLine(
                        "Client connected to " + client.getRemoteSocketAddress() +
                                " from " + client.getLocalSocketAddress());

                bStream = new BufferedOutputStream(client.getOutputStream(), 65536);
                DataOutput w = new LittleEndianDataOutputStream(bStream);
                w.writeLong(OUR_UUID.getLeastSignificantBits());
                w.writeLong(OUR_UUID.getMostSignificantBits());
                w.writeBoolean(conf.useCompression);

                stream = new BufferedOutputStream(conf.useCompression ? new GZIPOutputStream(bStream) : bStream, 65536);
                exhausted = writeRecordsBatchTo(stream);
            } catch (Exception e) {
                if(e instanceof InterruptedException || e instanceof InterruptedIOException)
                    return;
                Trace.writeLine("Failed to push messages: " + e);
                // Ignore: logging is not *that* important and we're not a persistent message queue.
                // Perhaps better luck during the next iteration.
            } finally {
                close(stream);
                close(bStream);
                close(client);
            }

            // Only sleep when waiting for new records.
            if (exhausted) {
                try {
                    Thread.sleep(conf.flushPeriodMs);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }

    private static void close(Closeable c) {
        if (c != null) {
            try {
                c.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }

    private static void close(Socket sock) {
        if (sock != null) {
            try {
                sock.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }

    private static boolean writeRecordsBatchTo(OutputStream stream) throws IOException {
        int maxBatchSize = 10000;
        DataOutput w = new LittleEndianDataOutputStream(stream);
        byte[] cidBytes = conf.clientId.getBytes("utf-8");
        w.writeInt(cidBytes.length);
        w.write(cidBytes);
        int recordsWritten = 0;

        byte[] machineBytes = hostname.getBytes("utf-8");

        CharsetEncoder enc = Charset.forName("utf-8").newEncoder();

        ByteBuffer maxMsg = ByteBuffer.allocate(1);
        for(Record rec : records) {
            w.writeInt(1);
            w.writeLong(rec.timestamp.toUtcNanos());
            w.writeInt(machineBytes.length);
            w.write(machineBytes);

            int maxLen = Math.round(rec.message.length() * enc.maxBytesPerChar() + 1);
            if(maxLen > maxMsg.limit()) {
                maxMsg = ByteBuffer.allocate(maxLen);
            }
            enc.reset();
            enc.encode(CharBuffer.wrap(rec.message), maxMsg, true);
            enc.flush(maxMsg);
            int bytesWritten = maxMsg.position();
            maxMsg.position(0);

            w.writeInt(bytesWritten);
            w.write(maxMsg.array(), maxMsg.arrayOffset(), bytesWritten);

            if(++recordsWritten == maxBatchSize)
                break;
        }
        w.writeInt(0);

        // Only remove records once we're sure that they have been written to server (no exception happened to this point)
        stream.flush();
        for(int i = 0; i < recordsWritten; ++i) {
            records.remove();
        }
        numRecords.addAndGet(-recordsWritten);

        Trace.writeLine("Written batch of " + recordsWritten + " records to greg");

        return recordsWritten < maxBatchSize;
    }

    private static void initiateCalibration() {
        while (!shouldTerminate()) {
            Socket client = null;
            try {
                client = new Socket(conf.server, conf.calibrationPort);
                client.setTcpNoDelay(true);
                exchangeTicksOver(client.getInputStream(), client.getOutputStream());
            } catch (Exception e) {
                if(e instanceof InterruptedIOException || e instanceof InterruptedException)
                    return;
                Trace.writeLine("Failed to exchange clock ticks during calibration, ignoring" + e);
            } finally {
                close(client);
            }
            try {
                Thread.sleep(conf.calibrationPeriodSec * 1000L);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private static void exchangeTicksOver(InputStream in, OutputStream out) throws IOException {
        DataOutput w = new LittleEndianDataOutputStream(out);
        DataInput r = new LittleEndianDataInputStream(in);
        w.writeLong(OUR_UUID.getLeastSignificantBits());
        w.writeLong(OUR_UUID.getMostSignificantBits());
        while (true) {
            // Here they measure their time and send it to us. It arrives after network latency.
            try {
                r.readLong(); // Their ticks
            } catch (EOFException e) {
                break;
            }
            w.writeLong(PreciseClock.INSTANCE.now().toUtcNanos());
            // Our sample arrives to them after network latency.
        }
    }
}
