package org.folio.reservoir.server;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.impl.future.FailedFuture;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.web.RoutingContext;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.okapi.common.HttpResponse;
import org.folio.reservoir.util.SourceId;
import org.folio.reservoir.util.readstream.Marc4ParserToJson;
import org.folio.reservoir.util.readstream.MarcXmlParserToJson;
import org.folio.reservoir.util.readstream.XmlParser;

public class UploadService {

  private static final Logger log = LogManager.getLogger(UploadService.class);

  private Future<Void> uploadOctetStream(ReadStream<Buffer> upload,
      IngestWriteStream ingestWriteStream) {
    log.info("uploadOctetStream");
    Promise<Void> promise = Promise.promise();
    Marc4ParserToJson marc4ParserToJson = new Marc4ParserToJson(upload);
    if (ingestWriteStream != null) {
      ingestWriteStream.drainHandler(x -> marc4ParserToJson.resume());
    }
    AtomicInteger number = new AtomicInteger();
    marc4ParserToJson.exceptionHandler(e -> {
      log.error("marc4jParser exception", e);
      promise.tryFail(e);
    });
    marc4ParserToJson.handler(r -> {
      if (number.incrementAndGet() < 10) {
        log.info("Got record controlnumber={}", r.getJsonArray("fields")
            .getJsonObject(0).getString("001"));
      } else if (number.get() % 10000 == 0) {
        log.info("Processed {}", number.get());
      }
      if (ingestWriteStream != null) {
        if (ingestWriteStream.writeQueueFull()) {
          marc4ParserToJson.pause();
        }
        ingestWriteStream.write(r)
            .onFailure(promise::tryFail);
      }
    });
    marc4ParserToJson.endHandler(e -> {
      log.info("{} records processed", number.get());
      promise.tryComplete();
    });
    return promise.future();
  }

  private Future<Void> uploadXmlStream(ReadStream<Buffer> upload,
      IngestWriteStream ingestWriteStream) {
    Promise<Void> promise = Promise.promise();
    XmlParser xmlParser = XmlParser.newParser(upload);
    MarcXmlParserToJson marcXmlParser = new MarcXmlParserToJson(xmlParser);
    marcXmlParser.exceptionHandler(e -> {
      log.error("marcXmlParser exception", e);
      promise.tryFail(e);
    });
    AtomicInteger number = new AtomicInteger();
    marcXmlParser.handler(marcInJson -> {
      if (number.incrementAndGet() < 10) {
        log.info("Got record controlnumber={}", marcInJson.getJsonArray("fields")
            .getJsonObject(0).getString("001"));
      } else if (number.get() % 10000 == 0) {
        log.info("Processed {}", number.get());
      }
      if (ingestWriteStream != null) {
        if (ingestWriteStream.writeQueueFull()) {
          marcXmlParser.pause();
        }
        ingestWriteStream.write(marcInJson)
            .onFailure(promise::tryFail);
      }
    });
    marcXmlParser.endHandler(e -> {
      log.info("{} records processed", number.get());
      promise.tryComplete();
    });
    return promise.future();
  }

  /**
   * non-OpenAPI upload records service handler.
   *
   * <p>Do not produce http response in case of failures.
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> uploadRecords(RoutingContext ctx) {
    try {
      log.info("uploadRecords");
      Storage storage = new Storage(ctx);
      HttpServerRequest request = ctx.request();
      String sourceId = request.getParam("sourceId");
      String sourceVersion = request.getParam("sourceVersion", "1");
      String localIdPath = request.getParam("localIdPath");
      final boolean ingest = request.getParam("ingest", "true").equals("true");
      final boolean raw = request.getParam("raw", "false").equals("true");
      IngestWriteStream ingestWriteStream = new IngestWriteStream(ctx.vertx(), storage,
          new SourceId(sourceId), Integer.parseInt(sourceVersion), localIdPath, "marc");
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
          switch (upload.contentType()) {
            case "application/octet-stream", "application/marc":
              futures.add(uploadOctetStream(upload, ingest ? ingestWriteStream : null));
              break;
            case "application/xml", "text/xml":
              futures.add(uploadXmlStream(upload, ingest ? ingestWriteStream : null));
              break;
            default:
              futures.add(new FailedFuture<>("Unsupported content-type: " + upload.contentType()));
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
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }
}
