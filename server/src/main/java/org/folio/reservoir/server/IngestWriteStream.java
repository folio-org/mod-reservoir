package org.folio.reservoir.server;

import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.WriteStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.reservoir.util.SourceId;
import org.marc4j.MarcJsonWriter;
import org.marc4j.converter.impl.AnselToUnicode;
import org.marc4j.marc.Record;

public class IngestWriteStream implements WriteStream<JsonObject> {

  private static final Logger log = LogManager.getLogger(IngestWriteStream.class);
  final SourceId sourceId;
  final int sourceVersion;
  final Storage storage;
  final Vertx vertx;
  Handler<Throwable> exceptionHandler;

  Handler<AsyncResult<Void>> endHandler;
  Handler<Void> drainHandler;
  JsonArray matchKeyConfigs;
  AtomicInteger ops = new AtomicInteger();
  final JsonPath jsonPath;
  int queueSize = 5;

  IngestWriteStream(Vertx vertx, Storage storage, SourceId sourceId, int sourceVersion,
      String localIdPath) {
    this.vertx = vertx;
    this.storage = storage;
    this.sourceId = sourceId;
    this.sourceVersion = sourceVersion;
    jsonPath = localIdPath == null ? null : JsonPath.compile(localIdPath);
  }

  @Override
  public WriteStream<JsonObject> exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public Future<Void> write(JsonObject globalRecord) {
    Future<Void> future = Future.succeededFuture();
    ops.incrementAndGet();
    if (matchKeyConfigs == null) {
      future = storage.getAvailableMatchConfigs().map(x -> {
        matchKeyConfigs = x;
        return null;
      });
    }
    return future
        .compose(y -> storage.ingestGlobalRecord(vertx, sourceId, sourceVersion,
            globalRecord, matchKeyConfigs)
        .onComplete(x -> {
          if (ops.decrementAndGet() == queueSize / 2) {
            if (drainHandler != null) {
              drainHandler.handle(null);
            }
          }
          if (ops.get() == 0 && endHandler != null) {
            endHandler.handle(Future.succeededFuture());
          }
        })
        .mapEmpty());
  }

  /**
   * Write marc record to stream.
   * @param marcRecord marc4j record
   * @return async result
   */
  public Future<Void> write(Record marcRecord) {
    ops.incrementAndGet();
    return vertx.<JsonObject>executeBlocking(globalRecord -> {
      try {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        MarcJsonWriter writer = new MarcJsonWriter(out);
        char charCodingScheme = marcRecord.getLeader().getCharCodingScheme();
        if (charCodingScheme == ' ') {
          marcRecord.getLeader().setCharCodingScheme('a');
          writer.setConverter(new AnselToUnicode());
        }
        writer.write(marcRecord);
        JsonObject marc = new JsonObject(out.toString());
        writer.close();
        // TODO consider delete records
        String localIdValue = null;
        JsonObject payload = new JsonObject().put("marc", marc);
        if (jsonPath != null) {
          localIdValue = getLocalIdValue(jsonPath, payload);
        }
        if (localIdValue == null) {
          localIdValue = marcRecord.getControlNumber();
        }
        globalRecord.complete(new JsonObject()
            .put("localId", localIdValue.trim())
            .put("payload", payload));
      } catch (Exception e) {
        globalRecord.tryFail(e);
      }
    }).compose(globalRecord -> {
      ops.decrementAndGet();
      return write(globalRecord);
    });
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

  static String getLocalIdValue(JsonPath jsonPath, JsonObject payload) {
    ReadContext ctx = JsonPath.parse(payload.encode());
    try {
      Object o = ctx.read(jsonPath);
      if (o instanceof String string) {
        return string;
      } else if (o instanceof List<?> list) {
        for (Object m : list) {
          if (m instanceof String string) {
            return string;
          }
        }
      }
    } catch (PathNotFoundException e) {
      // ignored.
    }
    return null;
  }

}
