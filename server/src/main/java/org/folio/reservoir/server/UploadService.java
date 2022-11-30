package org.folio.reservoir.server;

import com.jayway.jsonpath.InvalidPathException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
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
      IngestWriteStream ingestWriteStream, int queueSize) {
    Promise<Void> promise = Promise.promise();
    upload.endHandler(promise::tryComplete);
    upload.exceptionHandler(promise::tryFail);
    Pump.pump(upload, ingestWriteStream, queueSize).start();
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
      int queueSize = storage.pool.getPoolOptions().getMaxSize() * 10;
      log.info("Upload Content-Type {} source {} queueSize {}", contentType, sourceId, queueSize);
      Future<Void> future;
      if (contentType != null && contentType.startsWith("multipart/form-data")) {
        request.setExpectMultipart(true);
        List<Future<Void>> futures = new ArrayList<>();
        request.uploadHandler(upload ->
            futures.add(uploadContent(upload, ingestWriteStream, upload.contentType(),
                queueSize, raw))
        );
        Promise<Void> promise = Promise.promise();
        request.exceptionHandler(promise::tryFail);
        request.endHandler(e1 -> promise.handle(GenericCompositeFuture.all(futures).mapEmpty()));
        future = promise.future();
      } else {
        future = uploadContent(request, ingestWriteStream, contentType, queueSize, raw);
      }
      return future.onSuccess(response1 ->
          ingestWriteStream.end(s -> {
            JsonObject res = new JsonObject();
            HttpResponse.responseJson(ctx, 200).end(res.encode());
          }));
    } catch (InvalidPathException e) {
      return Future.failedFuture("malformed 'localIdPath': " + e.getMessage());
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  private Future<Void> uploadContent(ReadStream<Buffer> request,
      IngestWriteStream ingestWriteStream, String contentType, int queueSize, boolean raw) {
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
    return uploadContent(request, ingestWriteStream, contentType, queueSize);
  }

  private Future<Void> uploadContent(ReadStream<Buffer> request,
      IngestWriteStream ingestWriteStream, String contentType, int queueSize) {
    ReadStream<JsonObject> parser;
    if (contentType == null) {
      contentType = "application/octet-stream";
    }
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
    return uploadPayloadStream(parser, ingestWriteStream, queueSize);
  }
}
