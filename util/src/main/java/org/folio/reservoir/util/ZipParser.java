package org.folio.reservoir.util;

import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.DecodeException;
import io.vertx.core.streams.ReadStream;
import java.util.zip.Inflater;

public class ZipParser implements ReadStream<Buffer>, Handler<Buffer> {

  private static final int INFLATE_CHUNK = 4096;
  private long demand = Long.MAX_VALUE;
  private Handler<Throwable> exceptionHandler;
  private Handler<Buffer> eventHandler;
  private Handler<Void> endHandler;
  final ReadStream<Buffer> stream;
  final Inflater inflater = new Inflater();
  private boolean ended;
  private boolean emitting;

  ZipParser(ReadStream<Buffer> stream) {
    this.stream = stream;
  }

  private void checkPending()  {
    if (!emitting) {
      emitting = true;
      try {
        Buffer buffer = Buffer.buffer(INFLATE_CHUNK);
        while (demand > 0L) {
          byte [] output = new byte[INFLATE_CHUNK];
          int noBytes = inflater.inflate(output);
          if (noBytes == 0) {
            break;
          }
          if (demand != Long.MAX_VALUE) {
            --demand;
          }
          if (eventHandler != null) {
            buffer.appendBytes(output, 0, noBytes);
            eventHandler.handle(Buffer.buffer(output));
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
    try {
      inflater.inflate(buffer.getBytes());
    } catch (Exception e) {
      if (exceptionHandler != null) {
        exceptionHandler.handle(e);
      }
    }
    checkPending();
  }

  @Override
  public ReadStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public ReadStream<Buffer> handler(Handler<Buffer> handler) {
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
  public ReadStream<Buffer> pause() {
    demand = 0L;
    return this;
  }

  @Override
  public ReadStream<Buffer> resume() {
    return fetch(Long.MAX_VALUE);
  }

  @Override
  public ReadStream<Buffer> fetch(long l) {
    demand += l;
    if (demand < 0L) {
      demand = Long.MAX_VALUE;
    }
    checkPending();
    return this;
  }

  @Override
  public ReadStream<Buffer> endHandler(Handler<Void> handler) {
    if (!ended) {
      endHandler = handler;
    }
    return this;
  }

  private void end() {
    if (ended) {
      throw new IllegalStateException("Parsing already done");
    }
    inflater.end();
    ended = true;
    checkPending();
  }

}
