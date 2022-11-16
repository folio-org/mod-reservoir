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
import org.folio.reservoir.module.impl.ModuleJsonPath;
import org.folio.reservoir.util.SourceId;

public class IngestWriteStream implements WriteStream<JsonObject> {
  final SourceId sourceId;
  final int sourceVersion;
  final Storage storage;
  final Vertx vertx;
  Handler<Throwable> exceptionHandler;
  Handler<AsyncResult<Void>> endHandler;
  Handler<Void> drainHandler;
  JsonArray matchKeyConfigs;
  AtomicInteger ops = new AtomicInteger();
  final ModuleJsonPath moduleJsonPath;
  int queueSize = 5;

  IngestWriteStream(Vertx vertx, Storage storage, SourceId sourceId, int sourceVersion,
      String localIdPath) {
    this.vertx = vertx;
    this.storage = storage;
    this.sourceId = sourceId;
    this.sourceVersion = sourceVersion;
    moduleJsonPath = localIdPath == null ? null : new ModuleJsonPath(localIdPath);
  }

  @Override
  public WriteStream<JsonObject> exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public Future<Void> write(JsonObject payload) {
    final JsonObject globalRecord = new JsonObject().put("payload", payload);
    Future<Void> future = Future.succeededFuture();
    ops.incrementAndGet();
    if (matchKeyConfigs == null) {
      future = storage.getAvailableMatchConfigs().map(x -> {
        matchKeyConfigs = x;
        return null;
      });
    }
    return future
        .compose(x -> {
          JsonObject marc = payload.getJsonObject("marc");
          if (marc != null) {
            String leader = marc.getString("leader");
            if (leader.length() >= 24 && leader.charAt(5) == 'd') {
              globalRecord.put("delete", true);
            }
          }
          String v = null;
          if (moduleJsonPath != null) {
            Collection<String> strings = moduleJsonPath.executeAsCollection(null, payload);
            Iterator<String> iterator = strings.iterator();
            if (iterator.hasNext()) {
              v = iterator.next();
            }
          } else {
            v = getLocalIdFromMarc(marc);
          }
          if (v != null) {
            globalRecord.put("localId", v.trim());
          }
          return storage.ingestGlobalRecord(vertx, sourceId, sourceVersion,
              globalRecord, matchKeyConfigs);
        })
        .onComplete(x -> {
          if (ops.decrementAndGet() == queueSize / 2 && drainHandler != null) {
            drainHandler.handle(null);
          }
          if (ops.get() == 0 && endHandler != null) {
            endHandler.handle(Future.succeededFuture());
          }
        })
        .mapEmpty();
  }

  @Override
  public void write(JsonObject globalRecord, Handler<AsyncResult<Void>> handler) {
    write(globalRecord).onComplete(handler);
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    if (ops.get() == 0) {
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

  static String getLocalIdFromMarc(JsonObject marc) {
    if (marc == null) {
      return null;
    }
    JsonArray fields = marc.getJsonArray("fields");
    if (fields == null || fields.isEmpty()) {
      return null;
    }
    return fields.getJsonObject(0).getString("001");
  }

}
