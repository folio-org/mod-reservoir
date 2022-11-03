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
import org.folio.reservoir.util.Marc4jParser;
import org.folio.reservoir.util.SourceId;

public class UploadService {

  private static final Logger log = LogManager.getLogger(UploadService.class);

  private Future<Void> uploadOctetStream(ReadStream<Buffer> upload,
      IngestWriteStream ingestWriteStream) {
    log.info("uploadOctetStream");
    Promise<Void> promise = Promise.promise();
    try {
      Marc4jParser marc4jParser = new Marc4jParser(upload);
      if (ingestWriteStream != null) {
        ingestWriteStream.drainHandler(x -> marc4jParser.resume());
      }
      AtomicInteger number = new AtomicInteger();
      marc4jParser.exceptionHandler(e -> {
        log.error("marc4jParser exception", e);
        promise.tryFail(e);
      });
      marc4jParser.handler(r -> {
        if (number.incrementAndGet() < 10) {
          log.info("Got record leader={} controlnumber={}", r.getLeader(),
              r.getControlNumber().trim());
        } else if (number.get() % 10000 == 0) {
          log.info("Processed {}", number.get());
        }
        if (ingestWriteStream != null) {
          if (ingestWriteStream.writeQueueFull()) {
            marc4jParser.pause();
          }
          ingestWriteStream.write(r)
              .onFailure(e -> promise.tryFail(e));
        }
      });
      marc4jParser.endHandler(e -> {
        log.info("{} records processed", number.get());
        promise.tryComplete();
      });
    } catch (Exception e) {
      log.error("upload exception", e);
      promise.tryFail(e);
    }
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
          new SourceId(sourceId), Integer.parseInt(sourceVersion), localIdPath);
      ingestWriteStream.setWriteQueueMaxSize(100);
      List<Future<Void>> futures = new ArrayList<>();
      request.setExpectMultipart(true);
      request.exceptionHandler(e -> futures.add(new FailedFuture(e)));
      request.uploadHandler(upload -> {
        switch (upload.contentType()) {
          case "application/octet-stream":
          case "application/marc":
            if (raw) {
              AtomicLong sz = new AtomicLong();
              upload.handler(x -> sz.addAndGet(x.length()));
              upload.endHandler(end -> {
                log.info("Total size {}", sz.get());
              });
            } else {
              futures.add(uploadOctetStream(upload, ingest ? ingestWriteStream : null));
            }
            break;
          default:
            futures.add(new FailedFuture("Unsupported content-type: " + upload.contentType()));
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
