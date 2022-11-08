package org.folio.reservoir.util.readstream;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;

public class MemoryReadStream implements ReadStream<Buffer> {
  final Buffer preBuffer;
  final Buffer repeatBuffer;
  final Buffer sepBuffer;
  final Buffer postBuffer;
  final Vertx vertx;
  final int no;
  int stage = -1;
  boolean sep;
  boolean paused;
  Handler<Buffer> handler;
  Handler<Void> endHandler;

  public MemoryReadStream(Buffer preBuffer, Vertx vertx) {
    this(preBuffer, null, null, Buffer.buffer(), 0, vertx);
  }

  public MemoryReadStream(Buffer preBuffer, Buffer repeatBuffer, Buffer sepBuffer, Buffer postBuffer, int no, Vertx vertx) {
    this.preBuffer = preBuffer;
    this.repeatBuffer = repeatBuffer;
    this.sepBuffer = sepBuffer;
    this.postBuffer = postBuffer;
    this.no = no;
    this.vertx = vertx;
  }

  public void run() {
    if (paused) {
      return;
    }
    vertx.runOnContext(x -> {
      if (paused) {
        return;
      }
      if (stage < 0) {
        handler.handle(preBuffer);
        stage++;
      } else if (stage >= no) {
        if (stage > no) {
          return;
        }
        stage++;
        handler.handle(postBuffer);
        endHandler.handle(null);
      } else if (sep) {
        handler.handle(sepBuffer);
        sep = false;
      } else {
        handler.handle(repeatBuffer);
        if (stage < no - 1) {
          sep = true;
        }
        stage++;
      }
      if (!paused) {
        vertx.runOnContext(y -> run());
      }
    });
  }

  @Override
  public ReadStream<Buffer> exceptionHandler(Handler<Throwable> handler) {
    return this;
  }

  @Override
  public ReadStream<Buffer> handler(Handler<Buffer> handler) {
    this.handler = handler;
    return null;
  }

  @Override
  public ReadStream<Buffer> pause() {
    paused = true;
    return null;
  }

  @Override
  public ReadStream<Buffer> resume() {
    if (paused) {
      paused = false;
      run();
    }
    return null;
  }

  @Override
  public ReadStream<Buffer> fetch(long l) {
    return null;
  }

  @Override
  public ReadStream<Buffer> endHandler(Handler<Void> handler) {
    this.endHandler = handler;
    return null;
  }
}
