package org.folio.reservoir.server;

import com.jayway.jsonpath.InvalidPathException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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
            futures.add(uploadPayloadStream(parser, ingestWriteStream));
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
