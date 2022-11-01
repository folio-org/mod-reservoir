package org.folio.reservoir.util;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.streams.ReadStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.marc4j.MarcPermissiveStreamReader;
import org.marc4j.MarcReader;
import org.marc4j.marc.Record;

public class Marc4jParser implements ReadStream<Record>, Handler<Buffer> {
  private long demand = Long.MAX_VALUE;

  private boolean ended;

  private boolean emitting;

  private Handler<Throwable> exceptionHandler;

  private Handler<Record> eventHandler;

  private Handler<Void> endHandler;

  private final ReadStream<Buffer> stream;

  private Buffer pendingBuffer;

  /**
   * Construct marc4j streaming parser.
   * @param stream ISO2709 encoded byte stream
   */
  public Marc4jParser(ReadStream<Buffer> stream) {
    this.stream = stream;
    pendingBuffer = Buffer.buffer();
  }

  @Override
  public ReadStream<Record> exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public ReadStream<Record> handler(Handler<Record> handler) {
    eventHandler = handler;
    if (handler != null) {
      stream.handler(this);
      stream.endHandler(v -> end());
      stream.exceptionHandler(e -> {
        if (exceptionHandler != null) {
          exceptionHandler.handle(e);
        }
      });
    } else {
      stream.handler(null);
      stream.endHandler(null);
      stream.exceptionHandler(null);
    }
    return this;
  }

  @Override
  public ReadStream<Record> pause() {
    demand = 0L;
    return this;
  }

  @Override
  public ReadStream<Record> resume() {
    return fetch(Long.MAX_VALUE);
  }

  @Override
  public ReadStream<Record> fetch(long l) {
    demand += l;
    if (demand < 0L) {
      demand = Long.MAX_VALUE;
    }
    checkPending();
    return this;
  }

  @Override
  public ReadStream<Record> endHandler(Handler<Void> handler) {
    if (!ended) {
      endHandler = handler;
    }
    return this;
  }

  private Record getNext() {
    if (pendingBuffer.length() < 24) {
      return null;
    }
    String lead = pendingBuffer.getString(0, 5);
    int length = Integer.parseInt(lead);
    if (length < 24 || pendingBuffer.length() < length) {
      return null;
    }
    InputStream inputStream = new ByteArrayInputStream(pendingBuffer.getBytes(0, length));
    pendingBuffer = pendingBuffer.getBuffer(length, pendingBuffer.length());
    MarcReader marcReader = new MarcPermissiveStreamReader(inputStream, true, true);
    return marcReader.next();
  }

  private void checkPending()  {
    if (!emitting) {
      emitting = true;
      try {
        while (demand > 0L) {
          Record record = getNext();
          if (record == null) {
            break;
          }
          if (demand != Long.MAX_VALUE) {
            --demand;
          }
          if (eventHandler != null) {
            eventHandler.handle(record);
          }
        }
        if (ended) {
          Handler<Void> handler = endHandler;
          endHandler = null;
          if (handler != null) {
            handler.handle(null);
          }
        } else {
          if (demand == 0L) {
            stream.pause();
          } else {
            stream.resume();
          }
        }
      } catch (Exception e) {
        if (exceptionHandler != null) {
          exceptionHandler.handle(e);
        } else {
          throw new DecodeException(e.getMessage(), e);
        }
      } finally {
        emitting = false;
      }
    }
  }

  @Override
  public void handle(Buffer buffer) {
    this.pendingBuffer.appendBuffer(buffer);
    checkPending();
  }

  /**
   * Signal end-of-stream.
   */
  public void end() {
    if (ended) {
      throw new IllegalStateException("Parsing already done");
    }
    ended = true;
    checkPending();
  }

}
