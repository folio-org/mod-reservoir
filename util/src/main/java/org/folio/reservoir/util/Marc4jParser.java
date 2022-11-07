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

  private int getNext(int sz) {
    if (pendingBuffer.length() - sz < 24) {
      return 0;
    }
    // skip up to 5 non-digit bytes (bad data)
    for (int i = 0; i < 5; i++) {
      byte leadByte = pendingBuffer.getByte(sz);
      if (leadByte >= '0' && leadByte <= '9') {
        String lead = pendingBuffer.getString(sz, sz + 5);
        int length = Integer.parseInt(lead);
        return length < 24 || pendingBuffer.length() - sz < length ? 0 : length;
      }
      sz++;
    }
    return 0;
  }

  private void checkPending()  {
    if (emitting) {
      return;
    }
    emitting = true;
    try {
      int sz = 0;
      while (demand > 0L) {
        int add = getNext(sz);
        if (add == 0) {
          break;
        }
        sz += add;
        if (demand != Long.MAX_VALUE) {
          --demand;
        }
      }
      if (sz > 0) {
        try {
          InputStream inputStream = new ByteArrayInputStream(pendingBuffer.getBytes(0, sz));
          MarcReader marcReader = new MarcPermissiveStreamReader(inputStream, true, true);
          while (marcReader.hasNext()) {
            Record r = marcReader.next();
            if (eventHandler != null) {
              eventHandler.handle(r);
            }
          }
          pendingBuffer = pendingBuffer.getBuffer(sz, pendingBuffer.length());
        } catch (Exception e) {
          if (exceptionHandler != null) {
            exceptionHandler.handle(e);
          } else {
            throw new DecodeException(e.getMessage(), e);
          }
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
