package org.folio.reservoir.server;

import com.jayway.jsonpath.InvalidPathException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.impl.future.FailedFuture;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.Pump;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.web.RoutingContext;
import java.util.ArrayList;
import java.util.List;
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

  private static final Logger log = LogManager.getLogger(UploadService.class);

  private Future<Void> uploadPayloadStream(ReadStream<JsonObject> upload,
      IngestWriteStream ingestWriteStream) {
    Promise<Void> promise = Promise.promise();
    upload.endHandler(promise::tryComplete);
    upload.exceptionHandler(promise::tryFail);
    Pump.pump(upload, ingestWriteStream, 10).start();
    return promise.future();
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
          new SourceId(sourceId), Integer.parseInt(sourceVersion), ingest, jsonPath);
      ingestWriteStream.setWriteQueueMaxSize(100);
      String contentType = request.getHeader("Content-Type");
      log.info("Got Content-Type {}", request.getHeader("Content-Type"));
      if (contentType != null && contentType.startsWith("multipart/form-data")) {
        List<Future<Void>> futures = new ArrayList<>();
        request.setExpectMultipart(true);
        request.uploadHandler(upload -> {
          futures.add(uploadContent(upload, ingestWriteStream, upload.contentType(), raw));
        });
        request.exceptionHandler(e -> futures.add(new FailedFuture<>(e)));
        Promise<Void> promise = Promise.promise();
        request.endHandler(e1 ->
            ingestWriteStream.end(e2 ->
                promise.handle(GenericCompositeFuture.all(futures)
                    .map(s -> {
                      JsonObject res = new JsonObject();
                      HttpResponse.responseJson(ctx, 200).end(res.encode());
                      return null;
                    }))
            )
        );
        return promise.future();
      } else {
        return uploadContent(request, ingestWriteStream, contentType, raw)
            .compose(s -> {
              Promise<Void> promise = Promise.promise();
              ingestWriteStream.end(e2 -> {
                JsonObject res = new JsonObject();
                HttpResponse.responseJson(ctx, 200).end(res.encode());
                promise.tryComplete();
              });
              return promise.future();
            });
      }
    } catch (InvalidPathException e) {
      return Future.failedFuture("malformed 'localIdPath': " + e.getMessage());
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  private Future<Void> uploadContent(ReadStream<Buffer> request,
      IngestWriteStream ingestWriteStream, String contentType, boolean raw) {
    if (raw) {
      Promise<Void> promise = Promise.promise();
      AtomicLong sz = new AtomicLong();
      request.handler(x -> sz.addAndGet(x.length()));
      request.endHandler(end -> {
        log.info("Total size {}", sz.get());
        promise.complete();
      });
      return promise.future();
    }
    return uploadContent(request, ingestWriteStream, contentType);
  }

  private Future<Void> uploadContent(ReadStream<Buffer> request,
      IngestWriteStream ingestWriteStream, String contentType) {
    ReadStream<JsonObject> parser;
    switch (contentType) {
      case "application/octet-stream", "application/marc" ->
          parser = new MarcToJsonParser(request);
      case "application/xml", "text/xml" ->
          parser = new MarcXmlParserToJson(XmlParser.newParser(request));
      default -> {
        return Future.failedFuture("Unsupported content-type: " + contentType);
      }
    }
    parser = new MappingReadStream<>(parser, new MarcJsonToIngestMapper());
    return uploadPayloadStream(parser, ingestWriteStream);
  }
}
