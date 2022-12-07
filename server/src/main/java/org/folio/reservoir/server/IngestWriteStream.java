package org.folio.reservoir.server;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.WriteStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.reservoir.module.impl.ModuleJsonPath;
import org.folio.reservoir.util.SourceId;

public class IngestWriteStream implements WriteStream<JsonObject> {
  final SourceId sourceId;
  final int sourceVersion;
  final Storage storage;
  final Vertx vertx;
  Handler<Throwable> exceptionHandler;
  Handler<AsyncResult<Void>> endHandler;

  boolean ended;
  Handler<Void> drainHandler;
  JsonArray matchKeyConfigs;
  AtomicInteger ops = new AtomicInteger();
  int queueSize = 5;
  boolean ingest;
  ModuleJsonPath jsonPath;
  final IngestStats stats;
  private static final Logger log = LogManager.getLogger(IngestWriteStream.class);
  private static final String LOCAL_ID = "localId";

  IngestWriteStream(Vertx vertx, Storage storage, IngestParams params, String fileName) {
    this.vertx = vertx;
    this.storage = storage;
    sourceId = params.sourceId;
    sourceVersion = params.sourceVersion;
    ingest = params.ingest;
    jsonPath = params.jsonPath;
    this.stats = new IngestStats(fileName);
  }

  @Override
  public WriteStream<JsonObject> exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public Future<Void> write(JsonObject globalRecord) {
    ops.incrementAndGet();
    return lookupId(globalRecord)
        .compose(rec -> {
          log(rec);
          Future<Void> future = Future.succeededFuture();
          String localId = rec.getString(LOCAL_ID);
          if (ingest && localId != null) {
            if (matchKeyConfigs == null) {
              future = storage.getAvailableMatchConfigs().map(x -> {
                matchKeyConfigs = x;
                return null;
              });
            }
            future = future
                .compose(x -> storage
                  .ingestGlobalRecord(
                    vertx, sourceId, sourceVersion, rec, matchKeyConfigs)
                  .onSuccess(r -> {
                    if (r == null) {
                      stats.incrementDeleted();
                    } else if (r.booleanValue()) {
                      stats.incrementInserted();
                    } else {
                      stats.incrementUpdated();
                    }
                  }))
                .mapEmpty();
          }
          return future;
        })
        .onFailure(e -> {
          if (exceptionHandler != null) {
            exceptionHandler.handle(e);
          }
          ingest = false; // we report only error, so no need to ingest further
        })
        .onComplete(x -> {
          if (ops.decrementAndGet() == queueSize / 2 && drainHandler != null) {
            drainHandler.handle(null);
          }
          if (ops.get() == 0 && ended) {
            log.info("{} records processed", stats.processed());
            if (endHandler != null) {
              endHandler.handle(Future.succeededFuture());
            }
          }
        });
  }

  @Override
  public void write(JsonObject globalRecord, Handler<AsyncResult<Void>> handler) {
    write(globalRecord).onComplete(handler);
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    ended = true;
    if (ops.get() == 0) {
      log.info("{} records processed", stats.processed());
      handler.handle(Future.succeededFuture());
    } else {
      endHandler = handler;
    }
  }

  @Override
  public WriteStream<JsonObject> setWriteQueueMaxSize(int i) {
    queueSize = i;
    return this;
  }

  @Override
  public boolean writeQueueFull() {
    return ops.get() >= queueSize;
  }

  @Override
  public WriteStream<JsonObject> drainHandler(Handler<Void> handler) {
    drainHandler = handler;
    return this;
  }

  public IngestStats stats() {
    return stats;
  }

  private static Future<Collection<String>> lookupPath(Vertx vertx,
      ModuleJsonPath jsonPath, JsonObject payload) {
    return vertx.executeBlocking(p -> {
      Collection<String> strings = jsonPath.executeAsCollection(null, payload);
      p.complete(strings);
    },
    false);
  }

  private Future<JsonObject> lookupId(JsonObject rec) {
    Future<JsonObject> fut = Future.succeededFuture(rec);
    if (jsonPath != null) {
      fut = fut
        .compose(r ->
          lookupPath(vertx, jsonPath, r.getJsonObject("payload"))
            .map(strings -> {
              Iterator<String> iterator = strings.iterator();
              if (iterator.hasNext()) {
                r.put(LOCAL_ID, iterator.next().trim());
              } else {
                r.remove(LOCAL_ID);
              }
              return r;
            }
      ));
    }
    return fut;
  }

  private void log(JsonObject rec) {
    String localId = rec.getString(LOCAL_ID);
    if (stats.incrementProcessed() < 10) {
      if (localId != null) {
        log.info("Got record localId={}", localId);
      }
    } else if (stats.processed() % 10000 == 0) {
      log.info("Processed {}", stats.processed());
    }
    if (localId == null) {
      stats.incrementIgnored();
      log.warn("Record number {} without localId", stats.processed());
      if (stats.ignored() < 10) {
        log.warn("{}", rec::encodePrettily);
      }
    }
  }
}
