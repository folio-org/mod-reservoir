package org.folio.reservoir.server;

import com.jayway.jsonpath.InvalidPathException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.impl.future.FailedFuture;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.web.RoutingContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.okapi.common.HttpResponse;
import org.folio.reservoir.module.impl.ModuleJsonPath;
import org.folio.reservoir.util.SourceId;
import org.folio.reservoir.util.readstream.MappingReadStream;
import org.folio.reservoir.util.readstream.MarcJsonToIngestMapper;
import org.folio.reservoir.util.readstream.MarcToJsonParser;
import org.folio.reservoir.util.readstream.MarcXmlParserToJson;
import org.folio.reservoir.util.readstream.XmlParser;

public class UploadService {

  private static final String LOCAL_ID = "localId";
  private static final Logger log = LogManager.getLogger(UploadService.class);

  private Future<Void> uploadPayloadStream(Vertx vertx, ReadStream<JsonObject> upload,
      IngestWriteStream ingestWriteStream, ModuleJsonPath jsonPath) {
    Promise<Void> promise = Promise.promise();
    if (ingestWriteStream != null) {
      ingestWriteStream.drainHandler(x -> upload.resume());
    }
    AtomicInteger number = new AtomicInteger();
    AtomicInteger errors = new AtomicInteger();
    upload.exceptionHandler(promise::tryFail);
    upload.handler(r ->
        validateIngestRecord(vertx, jsonPath, r, number, errors) 
          .compose(rec -> {
            if (rec == null || ingestWriteStream == null) {
              return Future.succeededFuture(null);
            }
            if (ingestWriteStream.writeQueueFull()) {
              upload.pause();
            }
            return ingestWriteStream.write(rec);
          })
          .onFailure(promise::tryFail)
    );
    upload.endHandler(e -> {
      log.info("{} records processed", number.get());
      promise.tryComplete();
    });
    return promise.future();
  }

  private static Future<Collection<String>> lookupPath(Vertx vertx, 
      ModuleJsonPath jsonPath, JsonObject payload) {
    return vertx.executeBlocking(p -> { 
      Collection<String> strings = jsonPath.executeAsCollection(null, payload);
      p.complete(strings);
    }, 
    false);
  }

  private Future<JsonObject> validateIngestRecord(Vertx vertx, ModuleJsonPath jsonPath,
      JsonObject rec, AtomicInteger number, AtomicInteger errors) {
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
    return fut.map(r -> { 
      String localId = r.getString(LOCAL_ID);
      if (number.incrementAndGet() < 10) {
        if (localId != null) {
          log.info("Got record localId={}", localId);
        }
      } else if (number.get() % 10000 == 0) {
        log.info("Processed {}", number.get());
      }
      if (localId == null) {
        errors.incrementAndGet();
        log.warn("Record number {} without localId", number.get());
        if (errors.get() < 10) {
          log.warn("{}", r.encodePrettily());
        }
        return null;
      }
      return r;
    });
  }

  /**
   * non-OpenAPI upload records service handler.
   *
   * <p>Does not produce HTTP response in case of failures.
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> uploadRecords(RoutingContext ctx) {
    try {
      log.info("uploadRecords");
      Storage storage = new Storage(ctx);
      HttpServerRequest request = ctx.request();
      String sourceId = request.getParam("sourceId");
      if (sourceId == null) {
        return Future.failedFuture("sourceId is a required parameter");
      }
      String sourceVersion = request.getParam("sourceVersion", "1");
      final String localIdPath = request.getParam("localIdPath");
      final ModuleJsonPath jsonPath = localIdPath == null ? null : new ModuleJsonPath(localIdPath);
      final boolean ingest = request.getParam("ingest", "true").equals("true");
      final boolean raw = request.getParam("raw", "false").equals("true");
      IngestWriteStream ingestWriteStream = new IngestWriteStream(ctx.vertx(), storage,
          new SourceId(sourceId), Integer.parseInt(sourceVersion));
      ingestWriteStream.setWriteQueueMaxSize(100);
      List<Future<Void>> futures = new ArrayList<>();
      request.setExpectMultipart(true);
      request.exceptionHandler(e -> futures.add(new FailedFuture<>(e)));
      request.uploadHandler(upload -> {
        if (raw) {
          AtomicLong sz = new AtomicLong();
          upload.handler(x -> sz.addAndGet(x.length()));
          upload.endHandler(end -> log.info("Total size {}", sz.get()));
        } else {
          ReadStream<JsonObject> parser = null;
          log.info("Content-Type: {}", upload.contentType());
          switch (upload.contentType()) {
            case "application/octet-stream", "application/marc" ->
                parser = new MarcToJsonParser(upload);
            case "application/xml", "text/xml" ->
                parser = new MarcXmlParserToJson(XmlParser.newParser(upload));
            default -> futures.add(
                new FailedFuture<>("Unsupported content-type: " + upload.contentType())
            );
          }
          if (parser != null) {
            parser = new MappingReadStream<>(parser, new MarcJsonToIngestMapper());
            futures.add(uploadPayloadStream(
                ctx.vertx(), parser, ingest ? ingestWriteStream : null, jsonPath));
          }
        }
      });
      Promise<Void> promise = Promise.promise();
      request.endHandler(e1 ->
          ingestWriteStream.end(e2 ->
              promise.handle(GenericCompositeFuture.all(futures)
                  .compose(x -> ingestWriteStream.end())
                  .onSuccess(s -> {
                    JsonObject res = new JsonObject();
                    HttpResponse.responseJson(ctx, 200).end(res.encode());
                  }))
          )
      );
      return promise.future();
    } catch (InvalidPathException e) {
      return Future.failedFuture("malformed 'localIdPath': " + e.getMessage());
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }
}
