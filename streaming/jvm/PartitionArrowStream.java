package org.fgac.streaming;

import java.io.*;
import java.net.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.execution.arrow.ArrowBatchStreamWriter;
import scala.reflect.ClassTag$;

/** Opt-in Spark 3.5 local-mode adapter. No partition result is collected. */
public final class PartitionArrowStream {
  private static final ConcurrentHashMap<String, Handle> ACTIVE = new ConcurrentHashMap<>();
  public static Handle start(Dataset<?> frame) throws Exception {
    if (!frame.sparkSession().version().startsWith("3.5.") || !frame.sparkSession().sparkContext().isLocal())
      throw new IllegalArgumentException("PartitionArrowStream requires Spark 3.5 local mode");
    if (frame.sparkSession().sparkContext().getConf().getBoolean("spark.speculation", false))
      throw new IllegalArgumentException("Speculation is incompatible with incremental result delivery");
    Handle h = new Handle(frame); ACTIVE.put(h.id, h); h.writer.start(); return h;
  }
  private static final class Pump implements VoidFunction<Iterator<byte[]>> {
    private final String id;
    Pump(String id) { this.id = id; }
    public void call(Iterator<byte[]> it) throws Exception {
      Handle h = ACTIVE.get(id);
      if (h == null) throw new IllegalStateException("Local stream registry unavailable");
      TaskContext tc = TaskContext.get();
      if (tc.attemptNumber() != 0 || !h.partitions.add(tc.partitionId()))
        throw new IllegalStateException("Retried partitions must fail the stream; no duplicate delivery");
      while (it.hasNext()) {
        if (h.closed) throw new IOException("Consumer cancelled");
        byte[] batch = it.next(); h.firstGenerated.compareAndSet(0, System.nanoTime());
        if (batch.length > h.limit) throw new IOException("Arrow batch exceeds 16 MiB stream buffer limit");
        long t = System.nanoTime();
        while (!h.bytes.tryAcquire(batch.length, 100, TimeUnit.MILLISECONDS))
          if (h.closed) throw new IOException("Consumer cancelled");
        boolean offered = false;
        try {
          while (!(offered = h.queue.offer(batch, 100, TimeUnit.MILLISECONDS)))
            if (h.closed) throw new IOException("Consumer cancelled");
          h.peak.accumulateAndGet(h.queue.size(), Math::max);
        } finally { if (!offered) h.bytes.release(batch.length); }
        h.blockNs.addAndGet(System.nanoTime() - t);
      }
      h.firstPartitionDone.compareAndSet(0, System.nanoTime());
      h.lastPartitionDone.set(System.nanoTime());
    }
  }
  public static final class Handle {
    final String id = UUID.randomUUID().toString();
    final int limit = 16 * 1024 * 1024;
    final ArrayBlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(2);
    final Semaphore bytes = new Semaphore(limit);
    final Set<Integer> partitions = ConcurrentHashMap.newKeySet();
    final AtomicLong firstGenerated = new AtomicLong(), firstWritten = new AtomicLong(),
        firstPartitionDone = new AtomicLong(), lastPartitionDone = new AtomicLong(), blockNs = new AtomicLong();
    final AtomicInteger peak = new AtomicInteger();
    final long started = System.nanoTime();
    final ServerSocket listener;
    final String secret;
    final SparkContext sc;
    final String group;
    final Thread writer;
    volatile Thread runner;
    volatile Socket socket;
    volatile boolean closed, done;
    volatile Throwable failure;
    Handle(Dataset<?> frame) throws Exception {
      sc = frame.sparkSession().sparkContext(); group = sc.getLocalProperty("spark.jobGroup.id");
      if (group == null) throw new IllegalArgumentException("A cancellable Spark job group is required");
      Properties props = (Properties) sc.getLocalProperties().clone();
      listener = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1")); listener.setSoTimeout(30000);
      byte[] random = new byte[32]; new SecureRandom().nextBytes(random);
      StringBuilder sb = new StringBuilder(); for (byte b : random) sb.append(String.format("%02x", b & 255)); secret = sb.toString();
      writer = new Thread(() -> {
        try {
          socket = listener.accept(); socket.setSoTimeout(30000);
          byte[] received = new byte[64]; new DataInputStream(socket.getInputStream()).readFully(received);
          if (!MessageDigest.isEqual(received, secret.getBytes(java.nio.charset.StandardCharsets.US_ASCII)))
            throw new IOException("Invalid local stream token");
          OutputStream out = new BufferedOutputStream(socket.getOutputStream(), 64 * 1024);
          ArrowBatchStreamWriter arrow = new ArrowBatchStreamWriter(frame.schema(), out,
              frame.sparkSession().conf().get("spark.sql.session.timeZone"), false);
          out.flush();
          runner = new Thread(() -> {
            sc.setLocalProperties(props);
            try {
              JavaRDD<byte[]> rdd = JavaRDD.fromRDD(frame.toArrowBatchRdd(), ClassTag$.MODULE$.apply(byte[].class));
              rdd.foreachPartition(new Pump(id));
            } catch (Throwable ex) { failure = ex; }
            finally { done = true; sc.setLocalProperties(null); }
          }, "fgac-arrow-producer"); runner.setDaemon(true); runner.start();
          while (!done || !queue.isEmpty()) {
            if (closed) throw new IOException("Consumer cancelled");
            if (failure != null) throw new IOException("Spark producer failed", failure);
            byte[] batch = queue.poll(10, TimeUnit.MILLISECONDS);
            if (batch != null) {
              try { out.write(batch); out.flush(); firstWritten.compareAndSet(0, System.nanoTime()); }
              finally { bytes.release(batch.length); }
            }
          }
          if (failure != null) throw new IOException("Spark producer failed", failure);
          arrow.end(); out.flush();
        } catch (Throwable ex) { if (failure == null) failure = ex; sc.cancelJobGroup(group); }
        finally { closeSockets(); ACTIVE.remove(id); }
      }, "fgac-arrow-socket"); writer.setDaemon(true);
    }
    public int port() { return listener.getLocalPort(); }
    public String secret() { return secret; }
    public void awaitComplete() throws Exception {
      writer.join(120000);
      if (writer.isAlive()) throw new IOException("Stream completion timed out");
      if (failure != null) throw new IOException("Incremental Spark scan failed", failure);
      if (!done) throw new IOException("Spark scan did not complete");
    }
    public String metrics() {
      return "{\"first_generated_ms\":" + delta(firstGenerated.get()) + ",\"first_written_ms\":" + delta(firstWritten.get())
        + ",\"first_partition_done_ms\":" + delta(firstPartitionDone.get()) + ",\"last_partition_done_ms\":" + delta(lastPartitionDone.get())
        + ",\"producer_queue_ms\":" + blockNs.get()/1e6 + ",\"queue_peak_batches\":" + peak.get() + ",\"partitions\":" + partitions.size() + "}";
    }
    double delta(long value) { return value == 0 ? -1 : (value-started)/1e6; }
    void closeSockets() {
      try { listener.close(); } catch (IOException ignored) { }
      try { if (socket != null) socket.close(); } catch (IOException ignored) { }
    }
    public void close() {
      closed = true; closeSockets();
      if (!done) sc.cancelJobGroup(group);
      if (runner != null && runner.isAlive()) runner.interrupt();
      ACTIVE.remove(id);
    }
  }
}
