Why the name?
=============
Greg is for "Global Registrator". It is a scalable logger with a high-precision global time axis.

What is it for?
===============
  * Who hasn't **merged logs from different machines** and hasn't cursed their out-of-sync
    clocks and the offline mode of processing? 

  * Who hasn't wondered, **how much time it takes for a request to travel from one machine
    to the other**, and hasn't cursed his logs for not being able to provide this information?

  * Who hasn't wondered, how on earth one can **debug bottlenecks and find critical paths
    in latency-sensitive distributed systems**?

Greg solves these problems by being a global logger with a high-precision global time axis. 

It was written precisely because of the desperate need for such a tool and lack of an existing one.

_More precisely - I've had the idea of such a tool for a long time, then I wrote an implementation
for my employer, I liked it and decided to implement the same idea in open source._

What can I do with it?
======================
You can see that your http request arrived to the server 12ms after it departed from the client,
even if the client's and server's clocks are off by 3 hours. Just log "Request departed" at the
client and "Request arrived" at the server - you'll see their timestamps will differ by 12ms.

Or you can see that, when you were round-robining requests to 1000 machines through a message queue,
the last machine received the request 1.2 seconds after the first one, so your message queue handles
830 messages/sec.

Greg is especially useful in conjunction with the [timeplotters][1] tools.

How do I use it?
================
Download and unpack the distribution [from here][2] (haskell binding is available separately
[on hackage][3] - install it as `cabal install greg-client`).

Launch the server. 

    ./greg-server.sh (or greg-server.bat)

Use the client API from any machine (a single function).

For example, in Java (*bindings for several other languages coming soon, or you can easily write one yourself - see [Protocol]*):

    Greg.log("Request " + requestId + " departed");

Or use the Java log4j appender (`org.greg.client.log4j.GregAppender`).

The server will output records to stdout (redirect it somewhere yourself) in the following format (spaces are spaces, not tabs):

    MACHINE CLIENT_ID GLOBAL_TIMESTAMP MESSAGE

for example:

    DC03-016 SearchService 2010-11-20 14:06:35.782 Query arrived: rick astley

  * `MACHINE` is where the record was generated
  * `CLIENT_ID` is a user-specified identifier of the component that generated the record
  * `GLOBAL_TIMESTAMP` is global timestamp of the record, in the server's time zone
  * `MESSAGE` 

The server will do its best so that messages are logged in order of `GLOBAL_TIMESTAMP`.

See ConfigurationGuide for some options, in case you need them (you probably don't, except
for the option configuring server address and the CLIENT_ID).

You can restart the server at any time - clients will survive it (in the worst case, some
up to 1 second's worth of log messages from the client will be lost).

What if I like it?
==================
Then please:
  * Use it and tell me how it goes.
  * Write a client in your favourite language. It's easy! Shouldn't take more than a few hours - see [Protocol].

What if I don't like it?
========================
Then tell me how it could be made better.

How does it work?
=================
Greg is a client-server logger: you've got a single server and clients (bindings for several languages)
sending records to it. But do not fear: it's NOT like a remote syslog.

  * Greg registers events at the time of their occurence, not of arrival to the server;
  * Greg computes the clock offset between server and each client, thus aligning all records
    on a common global time axis with millisecond (or better) precision;
  * Greg uses a high-precision system clock;
  * Greg sends records in large batches and therefore it can handle a very high throughput of them
    (peak ~400,000 hello-world records per second - about 20MB/s) without sacrificing timing precision;
  * Greg was designed with robustness in mind and, if something fails, the whole thing doesn't crash;
    it is also designed to not crash under any kind of load (at worst, it will drop some messages).
  * Greg can handle thousands of clients.

_Note that this Java implementation has not been tested as extensively as the original one - but
some testing has been done, and it works basically in the same fashion. Please try it and tell me
if something's broken!_

Small script
------------

The significant assumption is that clients and server of Greg must be on a reasonably speedy LAN,
where network latencies are almost symmetric (it won't work well if it takes 1s for a packet
to travel in one direction and 300ms in the other - unfortunately, it is theoretically impossible
to work well under such circumstances, nor is it possible to detect them).

Why not X?
==========
  * **Why not ntpd + regular logging?**
    * You'd still need to somehow merge logs in off-line after collecting them on ntpd-synced machines,
      you can't "tail -f" the joint log.
    * Not all systems have ntpd installed.
    * Not on all systems, even where ntpd is installed, you actually have access to a system call which
      provides the current time with sufficient precision (for example, on Windows you don't, and
      on some Unixes you don't).
    * When using "precise system time", you're amenable to leaps of logged time at clock changes
      (like summer/winter time, or leap seconds) - on the contrary, Greg uses a continuous monotonic
      clock (an interval timer).
  * **Why not syslog?**
    * You'll still need ntpd for a global time axis - see notes about ntpd.
    * See note about high-precision timer for "regular logging".
    * syslog won't output the result in ascending order of global time.
    * Try piping 400,000 or even 50,000 messages per second into syslog.
  * **Why not Facebook's scribe?**
    * It does not provide a global time axis.


[1]: http://jkff.info/software/timeplotters
[2]: https://github.com/jkff/greg/tree/master/dist
[3]: http://hackage.haskell.org/package/greg-client
