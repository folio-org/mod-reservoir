package org.folio.reservoir.util.readstream;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.streams.ReadStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
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
    stream.handler(this);
    stream.endHandler(v -> end());
    stream.exceptionHandler(e -> {
      if (exceptionHandler != null) {
        exceptionHandler.handle(e);
      }
    });
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

  /**
   * Check if we have a complete MARC-record at buffer and return its length.
   * @param buffer memory buffer with presumably ISO2709 data
   * @param offset inspect at this offset and onwards.
   * @return 0 buffer does not hold complete MARC buffer; otherwise
   * return length of MARC record at buffer at offset.
   * @throws DecodeException if not a MARC record at offset
   */
  static int getNext(Buffer buffer, int offset) {
    // skip up to 4 non-digit bytes (bad data)
    for (int i = 0; i < 4; i++) {
      int remain = buffer.length() - offset;
      if (remain < 5) { // need at least 5 bytes for MARC header
        return 0;
      }
      byte leadByte = buffer.getByte(offset);
      if (leadByte >= '0' && leadByte <= '9') {
        String lead = buffer.getString(offset, offset + 5);
        int length = Integer.parseInt(lead);
        if (length < 24) {
          throw new DecodeException("Bad MARC length");
        }
        return remain >= length ? length : 0;
      }
      offset++;
    }
    throw new DecodeException("Missing MARC header");
  }

  private void checkPending()  {
    if (emitting) {
      return;
    }
    emitting = true;
    try {
      int sz = 0;
      while (demand > 0L) {
        int add = getNext(pendingBuffer, sz);
        if (add == 0) {
          break;
        }
        sz += add;
        if (demand != Long.MAX_VALUE) {
          --demand;
        }
      }
      if (sz > 0) {
        // have one or more MARC record to be parsed by Marc4j
        marc4jpending(sz);
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
        stream.handler(null); // only interested in first error
        exceptionHandler.handle(e);
      }
    } finally {
      emitting = false;
    }
  }

  private void marc4jpending(int sz) throws IOException {
    try (
        InputStream inputStream = new ByteArrayInputStream(pendingBuffer.getBytes(0, sz))
    ) {
      MarcReader marcReader = new MarcPermissiveStreamReader(inputStream, true, true);
      while (marcReader.hasNext()) {
        Record r = marcReader.next();
        if (eventHandler != null) {
          eventHandler.handle(r);
        }
      }
    } finally {
      pendingBuffer = pendingBuffer.getBuffer(sz, pendingBuffer.length());
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
