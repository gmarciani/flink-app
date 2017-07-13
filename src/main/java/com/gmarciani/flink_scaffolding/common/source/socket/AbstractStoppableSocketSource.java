/*
  The MIT License (MIT)

  Copyright (c) 2017 Giacomo Marciani

  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:


  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.


  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.
 */
package com.gmarciani.flink_scaffolding.common.source.socket;

import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class realizes ...
 *
 * @author Giacomo Marciani {@literal <gmarciani@acm.org>}
 * @since 1.0
 */
public abstract class AbstractStoppableSocketSource<T> implements SourceFunction<T> {

  private static final long serialVersionUID = 1L;

  protected static final Logger LOG = LoggerFactory.getLogger(AbstractStoppableSocketSource.class);

  /** Default delay between successive connection attempts. */
  protected static final int DEFAULT_CONNECTION_RETRY_SLEEP = 500;

  /** Default connection timeout when connecting to the server socket (infinite). */
  protected static final int CONNECTION_TIMEOUT_TIME = 0;

  protected final String hostname;
  protected final int port;
  protected final String delimiter;
  protected final long maxNumRetries;
  protected final long delayBetweenRetries;

  protected transient Socket currentSocket;

  protected volatile boolean isRunning = true;

  public AbstractStoppableSocketSource(String hostname, int port) {
    this(hostname, port, "\n", 0, DEFAULT_CONNECTION_RETRY_SLEEP);
  }

  public AbstractStoppableSocketSource(String hostname, int port, String delimiter, long maxNumRetries) {
    this(hostname, port, delimiter, maxNumRetries, DEFAULT_CONNECTION_RETRY_SLEEP);
  }

  public AbstractStoppableSocketSource(String hostname, int port, String delimiter, long maxNumRetries, long delayBetweenRetries) {
    checkArgument(port > 0 && port < 65536, "port is out of range");
    checkArgument(maxNumRetries >= -1, "maxNumRetries must be zero or larger (num retries), or -1 (infinite retries)");
    checkArgument(delayBetweenRetries >= 0, "delayBetweenRetries must be zero or positive");

    this.hostname = checkNotNull(hostname, "hostname must not be null");
    this.port = port;
    this.delimiter = delimiter;
    this.maxNumRetries = maxNumRetries;
    this.delayBetweenRetries = delayBetweenRetries;
  }

  /**
   * Starts the source. Implementations can use the {@link SourceContext} emit
   * elements.
   * <p>
   * <p>Sources that implement {@link Checkpointed}
   * must lock on the checkpoint lock (using a synchronized block) before updating internal
   * state and emitting elements, to make both an atomic operation:
   * <p>
   * <pre>{@code
   *  public class ExampleSource<T> implements SourceFunction<T>, Checkpointed<Long> {
   *      private long count = 0L;
   *      private volatile boolean isRunning = true;
   *
   *      public void run(SourceContext<T> ctx) {
   *          while (isRunning && count < 1000) {
   *              synchronized (ctx.getCheckpointLock()) {
   *                  ctx.collect(count);
   *                  count++;
   *              }
   *          }
   *      }
   *
   *      public void cancel() {
   *          isRunning = false;
   *      }
   *
   *      public Long snapshotState(long checkpointId, long checkpointTimestamp) { return count; }
   *
   *      public void restoreState(Long state) { this.count = state; }
   * }
   * }</pre>
   *
   * @param ctx The context to emit elements to and for accessing locks.
   */
  @Override
  public void run(SourceContext<T> ctx) throws Exception {
    final StringBuilder buffer = new StringBuilder();
    long attempt = 0;

    while (isRunning) {

      try (Socket socket = new Socket()) {
        currentSocket = socket;

        LOG.info("Connecting to server socket " + hostname + ':' + port);
        socket.connect(new InetSocketAddress(hostname, port), CONNECTION_TIMEOUT_TIME);
        BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        char[] cbuf = new char[8192];
        int bytesRead;
        while (isRunning && (bytesRead = reader.read(cbuf)) != -1) {
          buffer.append(cbuf, 0, bytesRead);
          int delimPos;
          while (buffer.length() >= delimiter.length() && (delimPos = buffer.indexOf(delimiter)) != -1) {
            String record = buffer.substring(0, delimPos);
            // truncate trailing carriage return
            if (delimiter.equals("\n") && record.endsWith("\r")) {
              record = record.substring(0, record.length() - 1);
            }

            this.handleRecord(record, ctx);

            buffer.delete(0, delimPos + delimiter.length());
          }
        }
      }

      // if we dropped out of this loop due to an EOF, sleep and retry
      if (isRunning) {
        attempt++;
        if (maxNumRetries == -1 || attempt < maxNumRetries) {
          LOG.warn("Lost connection to server socket. Retrying in " + delayBetweenRetries + " msecs...");
          Thread.sleep(delayBetweenRetries);
        }
        else {
          // this should probably be here, but some examples expect simple exists of the stream source
          // throw new EOFException("Reached end of stream and reconnects are not enabled.");
          break;
        }
      }
    }

    // collect trailing data
    if (buffer.length() > 0) {
      this.consumeTrailingData(buffer.toString(), ctx);
    }
  }

  /**
   * Cancels the source. Most sources will have a while loop inside the
   * {@link #run(SourceContext)} method. The implementation needs to ensure that the
   * source will break out of that loop after this method is called.
   * <p>
   * <p>A typical pattern is to have an {@code "volatile boolean isRunning"} flag that is set to
   * {@code false} in this method. That flag is checked in the loop condition.
   * <p>
   * <p>When a source is canceled, the executing thread will also be interrupted
   * (via {@link Thread#interrupt()}). The interruption happens strictly after this
   * method has been called, so any interruption handler can rely on the fact that
   * this method has completed. It is good practice to make any flags altered by
   * this method "volatile", in order to guarantee the visibility of the effects of
   * this method to any interruption handler.
   */
  @Override
  public void cancel() {
    isRunning = false;

    // we need to close the socket as well, because the Thread.interrupt() function will
    // not wake the thread in the socketStream.read() method when blocked.
    Socket theSocket = this.currentSocket;
    if (theSocket != null) {
      IOUtils.closeSocket(theSocket);
    }
  }

  public abstract void handleRecord(String record, SourceContext<T> ctx);

  public abstract void consumeTrailingData(String record, SourceContext<T> ctx);
}
