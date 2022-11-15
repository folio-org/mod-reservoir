package org.folio.reservoir.util.readstream;

import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;

public abstract class ReadStreamConverter<T,V> implements ReadStream<T>, Handler<V> {

  protected boolean emitting;

  protected long demand = Long.MAX_VALUE;

  protected boolean ended;

  protected final ReadStream<V> stream;

  protected Handler<T> eventHandler;

  protected Handler<Void> endHandler;

  protected Handler<Throwable> exceptionHandler;

  ReadStreamConverter(ReadStream<V> stream) {
    this.stream = stream;
    stream.handler(this);
    stream.endHandler(v -> end());
    stream.exceptionHandler(e -> {
      if (exceptionHandler != null) {
        exceptionHandler.handle(e);
      }
    });
  }

  @Override
  public ReadStream<T> exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public ReadStream<T> handler(Handler<T> handler) {
    eventHandler = handler;
    return this;
  }

  @Override
  public ReadStream<T> pause() {
    demand = 0L;
    return this;
  }

  @Override
  public ReadStream<T> resume() {
    return fetch(Long.MAX_VALUE);
  }

  @Override
  public ReadStream<T> fetch(long l) {
    demand += l;
    if (demand < 0L) {
      demand = Long.MAX_VALUE;
    }
    checkPending();
    return this;
  }

  @Override
  public ReadStream<T> endHandler(Handler<Void> handler) {
    if (!ended) {
      endHandler = handler;
    }
    return this;
  }

  void end() {
    if (ended) {
      throw new IllegalStateException("Parsing already done");
    }
    ended = true;
    checkPending();
  }

  void checkPending()  {
    if (emitting) {
      return;
    }
    emitting = true;
    try {
      handlePending();
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

  abstract void handlePending();

}
