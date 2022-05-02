package org.folio.metastorage.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.streams.ReadStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class ReadStreamConsumer<T, U> {
  private Promise<U> promise = Promise.promise();
  private List<Throwable> errors = new ArrayList<>();
  private AtomicInteger ongoing = new AtomicInteger();
  private AtomicBoolean completed = new AtomicBoolean();

  /**
   * Consumes provided ReadStream via the consumer function.
   * @param stream input stream
   * @param consumer consumer function
   * @return
   */
  public Future<U> consume(ReadStream<T> stream, Function<T, Future<U>> consumer) {
    stream
        .pause()
        .handler(r -> {
          ongoing.incrementAndGet();
          consumer.apply(r)
              .onComplete(x -> {
                ongoing.decrementAndGet();
                if (x.failed() && errors.isEmpty()) {
                  errors.add(x.cause());
                }
                finish();
              });
        })
        .endHandler(e -> {
          completed.set(Boolean.TRUE);
          finish();
        })
        .exceptionHandler(promise::fail)
        .resume();
    return promise.future();
  }

  private void finish() {
    if (completed.get() && ongoing.get() == 0) {
      if (errors.isEmpty()) {
        promise.complete();
      } else {
        promise.fail(errors.get(0));
      }
    }
  }
    
}
