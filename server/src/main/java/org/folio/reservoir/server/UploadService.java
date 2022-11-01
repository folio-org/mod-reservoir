package org.folio.reservoir.server;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.HttpResponse;
import org.folio.reservoir.util.Marc4jParser;

public class UploadService {

  private static final Logger log = LogManager.getLogger(UploadService.class);

  private void uploadOctetStream(Promise<Void> promise, ReadStream<Buffer> upload) {
    try {
      Marc4jParser marc4jParser = new Marc4jParser(upload);
      marc4jParser.exceptionHandler(e -> {
        log.error("marc4jParser exception", e);
        promise.tryFail(e);
      });
      marc4jParser.handler(r -> log.error("Got record leader={}", r.getLeader()));
      marc4jParser.endHandler(e -> log.error("All records processed"));
    } catch (Exception e) {
      log.error("upload exception", e);
      promise.tryFail(e);
    }
  }

  /**
   * non-OpenAPI upload records service handler.
   *
   * <p>Do not produce http response in case of failures.
   * @param ctx routing context
   * @return async result
   */
  public Future<Void> uploadRecords(RoutingContext ctx) {
    HttpServerRequest request = ctx.request();
    String sourceId = request.getParam("sourceId");
    String sourceVersion = request.getParam("sourceVersion");
    String localIdPath = request.getParam("localIdPath");
    Promise<Void> promise = Promise.promise();
    request.uploadHandler(upload -> {
      switch (upload.contentType()) {
        case "application/octet-stream":
        case "application/marc":
          uploadOctetStream(promise, upload);
          break;
        default:
          promise.tryFail("Unsupported content-type: " + upload.contentType());
      }
    });
    request.endHandler(e -> {
      if (promise.tryComplete()) {
        JsonObject res = new JsonObject();
        HttpResponse.responseJson(ctx, 200).end(res.encode());
      }
    });
    request.setExpectMultipart(true);
    return promise.future();
  }

}
